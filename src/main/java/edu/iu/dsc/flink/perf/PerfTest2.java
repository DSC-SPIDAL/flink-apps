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
import org.apache.flink.util.Collector;

import java.io.File;
import java.util.*;

public class PerfTest2 {
  public static void main(String[] args) throws Exception {
    // Checking input parameters
    final ParameterTool params = ParameterTool.fromArgs(args);
    boolean reduce = params.getBoolean("reduce", true);
    int parallel = params.getInt("parallel", 1);
    // set up execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface
    if (params.has("output")) {
      File f = new File(params.get("output"));
      f.delete();
    }
    // get input data:
    // read the points and centroids from the provided paths or fall back to default data
    DataSet<Point> points = getPointDataSet(params, env);
    DataSet<Centroid> centroids = getCentroidDataSet(params, env);

    // set number of bulk iterations for KMeans algorithm
    if (reduce) {
      System.out.println("##################### Reduce #########################");
      // set number of bulk iterations for KMeans algorithm
      IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

      DataSet<Centroid> newPoints = loop
          // compute closest centroid for each point
          .map(new RichMapFunction<Centroid, Tuple2<Integer, Centroid>>() {
            @Override
            public Tuple2<Integer, Centroid> map(Centroid point) throws Exception {
              Random r = new Random();
              for (int i = 0; i< 100000; i++) {
                double val = r.nextDouble() * r.nextDouble();
              }
              return new Tuple2<Integer, Centroid>(point.id, point);
            }
          }).setParallelism(parallel)
            .groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Centroid>, Tuple2<Integer, Centroid>>()  {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Centroid>> iterable, Collector<Tuple2<Integer, Centroid>> collector) throws Exception {
              Iterator<Tuple2<Integer, Centroid>> it = iterable.iterator();
              Map<Integer, Centroid> centroidMap = new HashMap<Integer, Centroid>();
              Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
              int index = -1;
              double x = 0, y = 0;
              int count = 0;
              while (it.hasNext()) {
                Tuple2<Integer, Centroid> p = it.next();
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
              }
              // System.out.println("Emitting: " + centroidMap.keySet().size());
              for (Map.Entry<Integer, Centroid> ce : centroidMap.entrySet()) {
                int c = counts.get(ce.getKey());
                collector.collect(new Tuple2<Integer, Centroid>(ce.getKey(), new Centroid(ce.getKey(), ce.getValue().x / c, ce.getValue().y / c)));
              }
            }
          }).setParallelism(parallel).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Centroid>, Centroid>()  {
            @Override
            public void reduce(Iterable<Tuple2<Integer, Centroid>> iterable, Collector<Centroid> collector) throws Exception {
              Iterator<Tuple2<Integer, Centroid>> it = iterable.iterator();
              Map<Integer, Centroid> centroidMap = new HashMap<Integer, Centroid>();
              Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
              int index = -1;
              double x = 0, y = 0;
              int count = 0;
              while (it.hasNext()) {
                Tuple2<Integer, Centroid> p = it.next();
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
              }
              // System.out.println("Emitting: " + centroidMap.keySet().size());
              for (Map.Entry<Integer, Centroid> ce : centroidMap.entrySet()) {
                int c = counts.get(ce.getKey());
                collector.collect(new Centroid(ce.getKey(), ce.getValue().x / c, ce.getValue().y / c));
              }
            }
          });

      // feed new centroids back into next iteration
      DataSet<Centroid> finalCentroids = loop.closeWith(newPoints);

      DataSet<Tuple2<Integer, Point>> clusteredPoints = points
          // assign points to final clusters
          .map(new KMeans.SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

      // emit result
      if (params.has("output")) {
        clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

        // since file sinks are lazy, we trigger the execution explicitly
        env.execute("KMeans Example");
      } else {
        System.out.println("Printing result to stdout. Use --output to specify output path.");
        clusteredPoints.print();
      }
    } else {
      System.out.println("##################### Not Reduce #########################");
      // set number of bulk iterations for KMeans algorithm
      IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));

      DataSet<Centroid> newPoints = loop
          // compute closest centroid for each point
          .map(new RichMapFunction<Centroid, Tuple2<Integer, Centroid>>() {
            @Override
            public Tuple2<Integer, Centroid> map(Centroid point) throws Exception {
              Random r = new Random();
              for (int i = 0; i< 100000; i++) {
                double val = r.nextDouble() * r.nextDouble();
              }
              return new Tuple2<Integer, Centroid>(point.id, new Centroid(point.id, point));
            }
          }).setParallelism(parallel).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Centroid>, Centroid>()  {
        @Override
        public void reduce(Iterable<Tuple2<Integer, Centroid>> iterable, Collector<Centroid> collector) throws Exception {
          Iterator<Tuple2<Integer, Centroid>> it = iterable.iterator();
          Map<Integer, Centroid> centroidMap = new HashMap<Integer, Centroid>();
          Map<Integer, Integer> counts = new HashMap<Integer, Integer>();
          int index = -1;
          double x = 0, y = 0;
          int count = 0;
          while (it.hasNext()) {
            Tuple2<Integer, Centroid> p = it.next();
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
          }
          // System.out.println("Emitting: " + centroidMap.keySet().size());
          for (Map.Entry<Integer, Centroid> ce : centroidMap.entrySet()) {
            int c = counts.get(ce.getKey());
            collector.collect(new Centroid(ce.getKey(), ce.getValue().x / c, ce.getValue().y / c));
          }
        }
      }).setParallelism(parallel);

      // feed new centroids back into next iteration
      DataSet<Centroid> finalCentroids = loop.closeWith(newPoints);

      DataSet<Tuple2<Integer, Point>> clusteredPoints = points
          // assign points to final clusters
          .map(new KMeans.SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

      // emit result
      if (params.has("output")) {
        clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");

        env.execute("KMeans Example");
      } else {
        System.out.println("Printing result to stdout. Use --output to specify output path.");
        clusteredPoints.print();
      }
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
      centroids = KMeansData.getDefaultCentroidDataSet(env, params.getInt("ccount", 100), params.getInt("parallel", 1));
    }
    return centroids;
  }

  private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
    DataSet<Point> points;
    if (params.has("points")) {
      points = env.readCsvFile(params.get("points"))
          .fieldDelimiter(" ")
          .pojoType(Point.class, "x", "y").setParallelism(params.getInt("parallel", 1));
    } else {
      System.out.println("Executing K-Means example with default point data set.");
      System.out.println("Use --points to specify file input.");
      points = KMeansData.getDefaultPointDataSet(env, params.getInt("pcount", 1000), params.getInt("parallel", 1));
    }
    return points;
  }
}
