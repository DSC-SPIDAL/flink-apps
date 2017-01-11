package edu.iu.dsc.flink.kmeans;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
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
            groupBy(0).combineGroup(new RichGroupCombineFunction<Tuple3<Integer, Point2, Integer>, Tuple3<Integer, Point2, Integer>>() {
          int dimentions;
          @Override
          public void open(Configuration parameters) throws Exception {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            dimentions = parameterTool.getInt("d", 100);
          }

          @Override
          public void combine(Iterable<Tuple3<Integer, Point2, Integer>> iterable,
                              Collector<Tuple3<Integer, Point2, Integer>> collector) throws Exception {
            Iterator<Tuple3<Integer, Point2, Integer>> it = iterable.iterator();
            int count = 0;
            int index = 0;
            double []zeros = new double[dimentions];
            Point2 point2 = new Point2(zeros);
            while (it.hasNext()) {
              Tuple3<Integer, Point2, Integer> p = it.next();
              point2.add(p.f1);
              index = p.f0;
              count += p.f2;
            }
            collector.collect(new Tuple3<>(index, point2, count));
          }
        })
        // count and sum point coordinates for each centroid
        .groupBy(0).reduceGroup(new RichGroupReduceFunction<Tuple3<Integer, Point2, Integer>, Centroid2>() {
          int dimentions;
          @Override
          public void open(Configuration parameters) throws Exception {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            dimentions = parameterTool.getInt("d", 100);
          }

          @Override
          public void reduce(Iterable<Tuple3<Integer, Point2, Integer>> iterable,
                             Collector<Centroid2> collector) throws Exception {
            Iterator<Tuple3<Integer, Point2, Integer>> it = iterable.iterator();
            int count = 0;
            int index = 0;
            double []zeros = new double[dimentions];
            Point2 point2 = new Point2(zeros);
            while (it.hasNext()) {
              Tuple3<Integer, Point2, Integer> p = it.next();
              point2.add(p.f1);
              index = p.f0;
              count += p.f2;
            }
            if (count != 0) {
              point2.div(count);
            }
            collector.collect(new Centroid2(index, point2));
          }
        });

    // feed new centroids back into next iteration
    DataSet<Centroid2> finalCentroids = loop.closeWith(newCentroids);

    // emit result
    if (params.has("output")) {
      finalCentroids.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);

      // since file sinks are lazy, we trigger the execution explicitly
      // env.setParallelism(1);
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
      points = env.readFile(new PointInputFormat(params.getInt("d", 100)), params.get("points")).setParallelism(params.getInt("parallel", 1));
    } else {
      System.out.println("Executing K-Means example with default point data set.");
      System.out.println("Use --points to specify file input.");
    }
    return points;
  }

  /**
   * Determines the closest cluster center for a data point.
   */
  public static final class SelectNearestCenter extends RichFlatMapFunction<PointBlock, Tuple3<Integer, Point2, Integer>> {
    private Collection<Centroid2> centroids;
    private Map<Integer, Point2> centroidMap;
    private Map<Integer, Integer> point;
    private int dimentions;
    /**
     * Reads the centroid values from a broadcast variable into a collection.
     */
    @Override
    public void open(Configuration parameters) throws Exception {
      ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
      dimentions = parameterTool.getInt("d", 100);
      this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
      centroidMap = new HashMap<Integer, Point2>();
      point = new HashMap<Integer, Integer>();

      for (Centroid2 c : centroids) {
        double []zeros = new double[dimentions];
        centroidMap.put(c.id, new Point2(zeros));
        point.put(c.id, 0);
      }
    }

    @Override
    public void flatMap(PointBlock block, Collector<Tuple3<Integer, Point2, Integer>> collector) throws Exception {
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
          zeros = new double[dimentions];
          collector.collect(new Tuple3<Integer, Point2, Integer>(ce.getKey(), ce.getValue(), 0));
        } else {
          collector.collect(new Tuple3<Integer, Point2, Integer>(ce.getKey(), ce.getValue(), count));
        }
      }
    }
  }
}
