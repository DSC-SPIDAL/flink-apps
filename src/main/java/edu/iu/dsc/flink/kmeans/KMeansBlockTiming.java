package edu.iu.dsc.flink.kmeans;

import edu.iu.dsc.flink.kmeans.utils.KMeansData;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

public class KMeansBlockTiming {
    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        int parallel = params.getInt("parallel", 1);
        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

        // get input data:
        // read the points and centroids from the provided paths or fall back to default data
        DataSet<PointBlock> points = getPointDataSet(params, env);
        DataSet<Centroid> centroids = getCentroidDataSet(params, env);

        // set number of bulk iterations for KMeans algorithm
        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<Centroid> newCentroids = points
                // compute closest centroid for each point
                .flatMap(new SelectNearestCenter()).withBroadcastSet(loop, "centroids").
                        groupBy(0).combineGroup(new RichGroupCombineFunction<Tuple2<Integer, Centroid>, Tuple2<Integer, Centroid>>() {

                    private int index = 0;
                    private int tasks = 0;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        index = getRuntimeContext().getIndexOfThisSubtask();
                        tasks = getRuntimeContext().getNumberOfParallelSubtasks();
                    }

                    @Override
                    public void combine(Iterable<Tuple2<Integer, Centroid>> iterable,
                                        Collector<Tuple2<Integer, Centroid>> collector) throws Exception {
                        long startTime = System.nanoTime();
                        Map<Integer, Centroid> centroidMap = new HashMap<Integer, Centroid>();
                        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
                        Iterator<Tuple2<Integer, Centroid>> it = iterable.iterator();
                        int index = -1;
                        int mapIndex = 0;
                        int count;
                        long time = 0;
                        long reductionTime = 0;
                        while (it.hasNext()) {
                            Tuple2<Integer, Centroid> p = it.next();
                            index = p.f0;
                            Centroid centroid;
                            if (centroidMap.containsKey(p.f0)) {
                                centroid = centroidMap.get(p.f0);
                                centroidMap.get(p.f0);
                                count = counts.get(p.f0);
                            } else {
                                centroid = new Centroid(p.f0, 0, 0);
                                centroidMap.put(p.f0, centroid);
                                count = 0;
                            }
                            count++;
                            centroid.x += p.f1.x;
                            centroid.y += p.f1.y;
                            time = p.f1.time;
                            reductionTime = p.f1.reductionTime;

                            counts.remove(p.f0);
                            counts.put(p.f0, count);
                        }
                        long endTime = System.nanoTime();
                        reductionTime += endTime - startTime;
                        for (Map.Entry<Integer, Centroid> ce : centroidMap.entrySet()) {
                            int c = counts.get(ce.getKey());
                            collector.collect(new Tuple2<Integer, Centroid>(ce.getKey(),
                                new Centroid(ce.getKey(), ce.getValue().x / c, ce.getValue().y / c, time, reductionTime)));
                        }
                    }
                })
                        // count and sum point coordinates for each centroid
                .groupBy(0).reduceGroup(new RichGroupReduceFunction<Tuple2<Integer, Centroid>, Centroid>() {
                    private int index = 0;
                    private int tasks = 0;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        index = getRuntimeContext().getIndexOfThisSubtask();
                        tasks = getRuntimeContext().getNumberOfParallelSubtasks();
                        if (index == 0) {
                            index = tasks;
                        }
                    }

                    @Override
                    public void reduce(Iterable<Tuple2<Integer, Centroid>> iterable,
                                       Collector<Centroid> collector) throws Exception {
                        long startTime = System.nanoTime();
                        Map<Integer, Centroid> centroidMap = new HashMap<Integer, Centroid>();
                        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
                        Iterator<Tuple2<Integer, Centroid>> it = iterable.iterator();
                        int index = -1;
                        int count = 0;
                        long time = 0;
                        long reductionTime = 0;
                        while (it.hasNext()) {
                            Tuple2<Integer, Centroid> p = it.next();
                            index = p.f0;
                            Centroid centroid;
                            if (centroidMap.containsKey(p.f0)) {
                                centroid = centroidMap.get(p.f0);
                                centroidMap.get(p.f0);
                                count = counts.get(p.f0);
                            } else {
                                centroid = new Centroid(index, 0, 0);
                                centroidMap.put(p.f0, centroid);
                                count = 0;
                            }
                            count++;
                            centroid.x += p.f1.x;
                            centroid.y += p.f1.y;
                            if (centroid.id == index || centroid.id % index == 0) {
                                time = p.f1.time;
                            }
                            reductionTime = p.f1.reductionTime;

                            counts.remove(p.f0);
                            counts.put(p.f0, count);
                        }
                        long endTime = System.nanoTime();
                        reductionTime += endTime - startTime;
                        for (Map.Entry<Integer, Centroid> ce : centroidMap.entrySet()) {
                            int c = counts.get(ce.getKey());
                            collector.collect(new Centroid(ce.getKey(), ce.getValue().x / c, ce.getValue().y / c, time, reductionTime));
                        }
                    }
                });

        // feed new centroids back into next iteration
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        // emit result
        if (params.has("output")) {
            finalCentroids.writeAsText(params.get("output"));

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KMeans Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            finalCentroids.print();
        }
    }

    private static DataSet<Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Centroid> centroids;
        if (params.has("centroids")) {
            centroids = env.readCsvFile(params.get("centroids"))
                    .fieldDelimiter(" ")
                    .pojoType(Centroid.class, "id", "x", "y").setParallelism(params.getInt("parallel", 1));
            ;
        } else {
            System.out.println("Executing K-Means example with default centroid data set.");
            System.out.println("Use --centroids to specify file input.");
            centroids = KMeansData.getDefaultCentroidDataSet(env);
        }
        return centroids;
    }

    private static DataSet<PointBlock> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<PointBlock> points;
        if (params.has("points")) {
            // read points from CSV file
            points = env.readFile(new PointInputFormat(), params.get("points")).setParallelism(params.getInt("parallel", 1));
        } else {
            System.out.println("Executing K-Means example with default point data set.");
            System.out.println("Use --points to specify file input.");
            points = KMeansData.getDefaultPointBlockDataSet(env);
        }
        return points;
    }

    /**
     * Determines the closest cluster center for a data point.
     */
    public static final class SelectNearestCenter extends RichFlatMapFunction<PointBlock, Tuple2<Integer, Centroid>> {
        private Collection<Centroid> centroids;
        private Map<Integer, Point> centroidMap;
        private Map<Integer, Integer> counts;
        private int index = 0;
        private int tasks = 0;
        /**
         * Reads the centroid values from a broadcast variable into a collection.
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
            index = getRuntimeContext().getIndexOfThisSubtask();
            tasks = getRuntimeContext().getNumberOfParallelSubtasks();
            centroidMap = new HashMap<Integer, Point>();
            counts = new HashMap<Integer, Integer>();
            if (index == 0) {
                index = tasks;
            }

            for (Centroid c : centroids) {
                centroidMap.put(c.id, new Point(0, 0));
                counts.put(c.id, 0);
            }
        }

        @Override
        public void flatMap(PointBlock block, Collector<Tuple2<Integer, Centroid>> collector) throws Exception {
            // check all cluster centers
            List<Point> points = block.points;
            long mapTime = 0;
            // we get the incoming time for this map
            for (Centroid centroid : centroids) {
                // compute distance
                if (centroid.id == index || centroid.id % index == 0) {
                    mapTime = centroid.time;
                    break;
                }
            }
            long time = System.nanoTime();
            for (Point p : points) {
                double minDistance = Double.MAX_VALUE;
                int closestCentroidId = -1;
                for (Centroid centroid : centroids) {
                    // compute distance
                    double distance = p.euclideanDistance(centroid);

                    // update nearest cluster if necessary
                    if (distance < minDistance) {
                        minDistance = distance;
                        closestCentroidId = centroid.id;
                    }
                }

                Point centroidPoint = centroidMap.get(closestCentroidId);
                int count;
                if (centroidPoint != null) {
                    centroidPoint.x += p.x;
                    centroidPoint.y += p.y;

                    count = counts.get(closestCentroidId);
                    count++;
                    counts.remove(closestCentroidId);
                    counts.put(closestCentroidId, count);
                }
            }
            long accuTime = (System.nanoTime() - time) + mapTime;
            // emit a new record with the center id and the data point.
            for (Map.Entry<Integer, Point> ce : centroidMap.entrySet()) {
                int c = counts.get(ce.getKey());
                if (c == 0) {
                    collector.collect(new Tuple2<Integer, Centroid>(ce.getKey(), new Centroid(ce.getKey(), index, 0f, 0f, accuTime)));
                } else {
                    collector.collect(new Tuple2<Integer, Centroid>(ce.getKey(), new Centroid(ce.getKey(), index, ce.getValue().x / c, ce.getValue().y / c, accuTime)));
                }
            }
        }
    }
}
