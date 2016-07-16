package edu.iu.dsc.flink.perf;

import edu.iu.dsc.flink.kmeans.Centroid;
import edu.iu.dsc.flink.kmeans.KMeans;
import edu.iu.dsc.flink.kmeans.Point;
import edu.iu.dsc.flink.kmeans.utils.KMeansData;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class PerfTest {
  public static void main(String[] args) throws Exception {
    // Checking input parameters
    final ParameterTool params = ParameterTool.fromArgs(args);
    boolean reduce = params.getBoolean("reduce", true);
    // set up execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

    // get input data:
    // read the points and centroids from the provided paths or fall back to default data
    DataSet<Point> points = getPointDataSet(params, env);

    // set number of bulk iterations for KMeans algorithm
    IterativeDataSet<Point> loop = points.iterate(params.getInt("iterations", 10));
    reduce = true;
    if (reduce) {
      points = loop
          .map(new RichMapFunction<Point, Tuple2<Integer, Point>>() {
            private int counter = 0;
            int reduction = 0;

            /**
             * Reads the centroid values from a broadcast variable into a collection.
             */
            @Override
            public void open(Configuration parameters) throws Exception {
              reduction = parameters.getInteger("reduction", 1);
            }

            @Override
            public Tuple2<Integer, Point> map(Point p) throws Exception {
              // emit a new record with the center id and the data point.
              return new Tuple2<>(counter++ % reduction, p);
            }
          })
          // count and sum point coordinates for each centroid
          .groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Point>, Point>() {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Point>> iterable,
                               Collector<Point> collector) throws Exception {
              for (Tuple2<Integer, Point> p : iterable) {
                collector.collect(new Point(p.f1.x, p.f1.y));
              }
            }
          });
    } else {
      points = loop
          .map(new RichMapFunction<Point, Point>() {
               int reduction = 0;
               /**
                * Reads the centroid values from a broadcast variable into a collection.
                */
               @Override
               public void open(Configuration parameters) throws Exception {
                 reduction = parameters.getInteger("reduction", 1);
               }

               @Override
               public Point map(Point p) throws Exception {
                 // emit a new record with the center id and the data point.
                 return p;
               }
             }
          );
    }

    // feed new centroids back into next iteration
    DataSet<Point> finalPoints = loop.closeWith(points);

    // emit result
    if (params.has("output")) {
      finalPoints.writeAsCsv(params.get("output"), "\n", " ");

      // since file sinks are lazy, we trigger the execution explicitly
      env.execute("KMeans Example");
    } else {
      System.out.println("Printing result to stdout. Use --output to specify output path.");
      finalPoints.print();
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
          .pojoType(Centroid.class, "id", "x", "y").setParallelism(params.getInt("parallel", 1));
      ;
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
}
