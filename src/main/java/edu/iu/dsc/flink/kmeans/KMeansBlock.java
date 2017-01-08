package edu.iu.dsc.flink.kmeans;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.*;

public class KMeansBlock {
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
    DataSet<String> centroidLines = getDataSet(params.get("centroids"), env);
    DataSet<Centroid2> centroids = centroidLines.map(new RichMapFunction<String, Centroid2>() {
      @Override
      public Centroid2 map (String s) throws Exception {
        String[] split = s.split(" ");
        double[] values = new double[split.length - 1];
        for (int i = 1; i < split.length; i++) {
          values[i - 1] = Double.parseDouble(split[i]);
        }
        return new Centroid2(Integer.parseInt(split[0]), values);
      }
    });

    // set number of bulk iterations for KMeans algorithm
    IterativeDataSet<Centroid2> loop = centroids.iterate(params.getInt("iterations", 10));

    DataSet<Centroid2> newCentroids = points
        // compute closest centroid for each point
        .flatMap(new SelectNearestCenter()).withBroadcastSet(loop, "centroids").
            groupBy(0).combineGroup(new GroupCombineFunction<Tuple2<Integer, Point2>, Tuple2<Integer, Point2>>() {
          @Override
          public void combine(Iterable<Tuple2<Integer, Point2>> iterable,
                              Collector<Tuple2<Integer, Point2>> collector) throws Exception {
            Map<Integer, Centroid2> centroidMap = new HashMap<Integer, Centroid2>();
            Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
            Iterator<Tuple2<Integer, Point2>> it = iterable.iterator();
            int index;
            int count;
            while (it.hasNext()) {
              Tuple2<Integer, Point2> p = it.next();
              index = p.f0;
              Centroid2 centroid;
              if (centroidMap.containsKey(p.f0)) {
                centroid = centroidMap.get(p.f0);
                centroidMap.get(p.f0);
                count = counts.get(p.f0);
              } else {
                double []zeros;
                zeros = new double[100];
                centroid = new Centroid2(index, zeros);
                centroidMap.put(p.f0, centroid);
                count = 0;
              }
              count++;
              centroid.add(p.f1);

              counts.put(p.f0, count);
            }
            for (Map.Entry<Integer, Centroid2> ce : centroidMap.entrySet()) {
              int c = counts.get(ce.getKey());
              collector.collect(new Tuple2<Integer, Point2>(ce.getKey(),
                  ce.getValue().div(c)));
            }
          }
        })
        // count and sum point coordinates for each centroid
        .groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Point2>, Centroid2>() {
          @Override
          public void reduce(Iterable<Tuple2<Integer, Point2>> iterable,
                             Collector<Centroid2> collector) throws Exception {
            Map<Integer, Centroid2> centroidMap = new HashMap<Integer, Centroid2>();
            Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
            Iterator<Tuple2<Integer, Point2>> it = iterable.iterator();
            int index;
            int count;
            while (it.hasNext()) {
              Tuple2<Integer, Point2> p = it.next();
              index = p.f0;
              Centroid2 centroid;
              if (centroidMap.containsKey(p.f0)) {
                centroid = centroidMap.get(p.f0);
                centroidMap.get(p.f0);
                count = counts.get(p.f0);
              } else {
                double []zeros;
                zeros = new double[100];
                centroid = new Centroid2(index, zeros);
                centroidMap.put(p.f0, centroid);
                count = 0;
              }
              count++;
              centroid.add(p.f1);

              counts.remove(p.f0);
              counts.put(p.f0, count);
            }
            for (Map.Entry<Integer, Centroid2> ce : centroidMap.entrySet()) {
              int c = counts.get(ce.getKey());
              collector.collect(new Centroid2(ce.getKey(), ce.getValue().div(c)));
            }
          }
        });

    // feed new centroids back into next iteration
    DataSet<Centroid2> finalCentroids = loop.closeWith(newCentroids);

    // emit result
    if (params.has("output")) {
      finalCentroids.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);

      // since file sinks are lazy, we trigger the execution explicitly
      env.execute("KMeans Example");
      System.out.println("Total time: " + env.getLastJobExecutionResult().getNetRuntime());
    } else {
      System.out.println("Printing result to stdout. Use --output to specify output path.");
      finalCentroids.print();
    }
  }

  private static DataSet<String> getDataSet(String file, ExecutionEnvironment env) {
    DataSet<String> points = null;
    points = env.readTextFile(file);
    return points;
  }

  private static DataSet<PointBlock> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
    DataSet<PointBlock> points = null;
    if (params.has("points")) {
      // read points from CSV file
      points = env.readFile(new PointInputFormat(), params.get("points")).setParallelism(params.getInt("parallel", 1));
    } else {
      System.out.println("Executing K-Means example with default point data set.");
      System.out.println("Use --points to specify file input.");
    }
    return points;
  }

  /**
   * Determines the closest cluster center for a data point.
   */
  public static final class SelectNearestCenter extends RichFlatMapFunction<PointBlock, Tuple2<Integer, Point2>> {
    private Collection<Centroid2> centroids;
    private Map<Integer, Point2> centroidMap;
    private Map<Integer, Integer> point;
    /**
     * Reads the centroid values from a broadcast variable into a collection.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
      this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
      centroidMap = new HashMap<Integer, Point2>();
      point = new HashMap<Integer, Integer>();

      for (Centroid2 c : centroids) {
        double []zeros;
        zeros = new double[100];
        centroidMap.put(c.id, new Point2(zeros));
        point.put(c.id, 0);
      }
    }

    @Override
    public void flatMap(PointBlock block, Collector<Tuple2<Integer, Point2>> collector) throws Exception {
      // check all cluster centers
      List<Point2> points = block.points;
      for (Point2 p : points) {
        double minDistance = Double.MAX_VALUE;
        int closestCentroidId = -1;
        for (Centroid2 centroid : centroids) {
          // compute distance
          double distance = p.euclideanDistance(centroid);

          // update nearest cluster if necessary
          if (distance < minDistance) {
            minDistance = distance;
            closestCentroidId = centroid.id;
          }
        }

        Point2 centroidPoint = centroidMap.get(closestCentroidId);
        int count;
        if (centroidPoint != null) {
          centroidPoint.add(p);

          count = point.get(closestCentroidId);
          count++;
          point.remove(closestCentroidId);
          point.put(closestCentroidId, count);
        }
      }
      // emit a new record with the center id and the data point.
      for (Map.Entry<Integer, Point2> ce : centroidMap.entrySet()) {
        int count = point.get(ce.getKey());
        if (count == 0) {
          double []zeros;
          zeros = new double[100];
          collector.collect(new Tuple2<Integer, Point2>(ce.getKey(), new Point2(zeros)));
        } else {
          collector.collect(new Tuple2<Integer, Point2>(ce.getKey(), ce.getValue().div(count)));
        }
      }
    }
  }
}
