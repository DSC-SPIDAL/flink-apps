package edu.iu.dsc.flink.kmeans;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.iu.dsc.flink.kmeans.utils.KMeansData;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.util.Collector;

/**
 * This example implements a basic K-Means clustering algorithm.
 *
 * <p>
 * K-Means is an iterative clustering algorithm and works as follows:<br>
 * K-Means is given a set of data points to be clustered and an initial set of <i>K</i> cluster centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (<i>mean</i>) of all points that have been assigned to it.
 * The moved cluster centers are fed into the next iteration.
 * The algorithm terminates after a fixed number of iterations (as in this implementation)
 * or if cluster centers do not (significantly) move in an iteration.<br>
 * This is the Wikipedia entry for the <a href="http://en.wikipedia.org/wiki/K-means_clustering">K-Means Clustering algorithm</a>.
 *
 * <p>
 * This implementation works on two-dimensional data points. <br>
 * It computes an assignment of data points to cluster centers, i.e.,
 * each data point is annotated with the id of the final cluster (center) it belongs to.
 *
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as two double values separated by a blank character.
 * Data points are separated by newline characters.<br>
 * For example <code>"1.2 2.3\n5.3 7.2\n"</code> gives two data points (x=1.2, y=2.3) and (x=5.3, y=7.2).
 * <li>Cluster centers are represented by an integer id and a point value.<br>
 * For example <code>"1 6.2 3.2\n2 2.9 5.7\n"</code> gives two centers (id=1, x=6.2, y=3.2) and (id=2, x=2.9, y=5.7).
 * </ul>
 *
 * <p>
 * Usage: <code>KMeans --points &lt;path&gt; --centroids &lt;path&gt; --output &lt;path&gt; --iterations &lt;n&gt;</code><br>
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>Bulk iterations
 * <li>Broadcast variables in bulk iterations
 * <li>Custom Java objects (POJOs)
 * </ul>
 */
@SuppressWarnings("serial")
public class KMeans {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        int parallel = params.getInt("parallel", 1);
        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

        // get input data:
        // read the points and centroids from the provided paths or fall back to default data
        DataSet<Point> points = getPointDataSet(params, env);
        DataSet<Centroid> centroids = getCentroidDataSet(params, env);

        // set number of bulk iterations for KMeans algorithm
        IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

        DataSet<Centroid> newCentroids = points
                // compute closest centroid for each point
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids").
                        groupBy(0).combineGroup(new GroupCombineFunction<Tuple2<Integer, Point>, Tuple2<Integer, Point>>() {
                    @Override
                    public void combine(Iterable<Tuple2<Integer, Point>> iterable,
                                        Collector<Tuple2<Integer, Point>> collector) throws Exception {
                        Iterator<Tuple2<Integer, Point>> it = iterable.iterator();
                        int index = -1;
                        double x = 0, y = 0;
                        int count = 0;
                        long time = 0;
                        while (it.hasNext()) {
                            Tuple2<Integer, Point> p = it.next();
                            x += p.f1.x;
                            y += p.f1.y;
                            index = p.f0;
                            count++;
                            time = p.f1.time;
                        }
                        collector.collect(new Tuple2<Integer, Point>(index, new Point(x / count, y / count, time)));
                    }
                })
                 // count and sum point coordinates for each centroid
                .reduceGroup(new GroupReduceFunction<Tuple2<Integer, Point>, Centroid>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, Point>> iterable,
                                       Collector<Centroid> collector) throws Exception {
                        Map<Integer, Centroid> centroidMap = new HashMap<Integer, Centroid>();
                        Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
                        Iterator<Tuple2<Integer, Point>> it = iterable.iterator();
                        int index = -1;
                        double x = 0, y = 0;
                        int count = 0;
                        long time = 0;
                        while (it.hasNext()) {
                            Tuple2<Integer, Point> p = it.next();
                            x += p.f1.x;
                            y += p.f1.y;
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
                            counts.remove(p.f0);
                            counts.put(p.f0, count);
                            time = p.f1.time;
                        }
                        for (Map.Entry<Integer, Centroid> ce : centroidMap.entrySet()) {
                            int c = counts.get(ce.getKey());
                            collector.collect(new Centroid(ce.getKey(), ce.getValue().x / c, ce.getValue().y / c, time));
                        }
                    }
                });

        // feed new centroids back into next iteration
        DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

        DataSet<Tuple2<Integer, Point>> clusteredPoints = points
                // assign points to final clusters
                .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

        // emit result
        if (params.has("output")) {
            clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KMeans Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            clusteredPoints.print();
        }
    }

    // *************************************************************************
    //     DATA SOURCE READING (POINTS AND CENTROIDS)
    // *************************************************************************

    private static DataSet<Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Centroid> centroids;
        if (params.has("centroids")) {
            centroids = env.readCsvFile(params.get("centroids"))
                    .fieldDelimiter(" ")
                    .pojoType(Centroid.class, "id", "x", "y").setParallelism(params.getInt("parallel", 1));;
        } else {
            System.out.println("Executing K-Means example with default centroid data set.");
            System.out.println("Use --centroids to specify file input.");
            centroids = KMeansData.getDefaultCentroidDataSet(env);
        }
        return centroids;
    }

    private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Point> points;
        if (params.has("points")) {
            // read points from CSV file
            points = env.readCsvFile(params.get("points"))
                    .fieldDelimiter(" ")
                    .pojoType(Point.class, "x", "y").setParallelism(params.getInt("parallel", 1));
        } else {
            System.out.println("Executing K-Means example with default point data set.");
            System.out.println("Use --points to specify file input.");
            points = KMeansData.getDefaultPointDataSet(env);
        }
        return points;
    }

    // *************************************************************************
    //     DATA TYPES
    // *************************************************************************

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Determines the closest cluster center for a data point. */
    @ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
        private Collection<Centroid> centroids;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
            System.out.println("No of centroids: " + centroids.size());
        }

        @Override
        public Tuple2<Integer, Point> map(Point p) throws Exception {
            long time = System.nanoTime();
            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }
            long computeTIme = System.nanoTime() - time;
            p.time += computeTIme;
            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }
}
