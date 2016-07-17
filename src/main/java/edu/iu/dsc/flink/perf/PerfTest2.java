//package edu.iu.dsc.flink.perf;
//
//import edu.iu.dsc.flink.kmeans.Centroid;
//import edu.iu.dsc.flink.kmeans.KMeans;
//import edu.iu.dsc.flink.kmeans.Point;
//import edu.iu.dsc.flink.kmeans.utils.KMeansData;
//import org.apache.flink.api.common.functions.*;
//import org.apache.flink.api.common.typeinfo.IntegerTypeInfo;
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.functions.FunctionAnnotation;
//import org.apache.flink.api.java.operators.IterativeDataSet;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.util.Collector;
//
//import java.io.File;
//import java.util.Collection;
//import java.util.Iterator;
//import java.util.Random;
//
//public class PerfTest2 {
//  public static void main(String[] args) throws Exception {
//    // Checking input parameters
//    final ParameterTool params = ParameterTool.fromArgs(args);
//    boolean reduce = params.getBoolean("reduce", true);
//    // set up execution environment
//    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//    env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface
//    if (params.has("output")) {
//      File f = new File(params.get("output"));
//      f.delete();
//    }
//    // get input data:
//    // read the points and centroids from the provided paths or fall back to default data
//    DataSet<Point> points = getPointDataSet(params, env);
//    DataSet<Centroid> centroids = getCentroidDataSet(params, env);
//
//    // set number of bulk iterations for KMeans algorithm
//    if (reduce) {
//      System.out.println("##################### Reduce #########################");
//      // set number of bulk iterations for KMeans algorithm
//      IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));
//
//      DataSet<Centroid> newPoints = loop
//          // compute closest centroid for each point
//          .map(new RichMapFunction<Centroid, Tuple2<Integer, Centroid>>() {
//            @Override
//            public Tuple2<Integer, Centroid> map(Centroid point) throws Exception {
//              return new Tuple2<Integer, Centroid>();
//            }
//          })
//              .groupBy(0).combineGroup(new GroupCombineFunction<Tuple2<Integer, Centroid>, Tuple2<Integer, Centroid>>() {
//            @Override
//            public void combine(Iterable<Tuple2<Integer, Centroid>> iterable,
//                                Collector<Tuple2<Integer, Centroid>> collector) throws Exception {
//              Iterator<Tuple2<Integer, Centroid>> it = iterable.iterator();
//              int index = -1;
//              double x = 0, y = 0;
//              int count = 0;
//              while (it.hasNext()) {
//                Tuple2<Integer, Centroid> p = it.next();
//                x += p.f1.x;
//                y += p.f1.y;
//                index = p.f0;
//                count++;
//              }
//              collector.collect(new Tuple2<Integer, Centroid>(index, new Centroid(index, x / count, y / count)));
//            }
//          })
//          // count and sum point coordinates for each centroid
//          .groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer, Point>, Centroid>() {
//            @Override
//            public void reduce(Iterable<Tuple2<Integer, Point>> iterable,
//                               Collector<Centroid> collector) throws Exception {
//              Iterator<Tuple2<Integer, Point>> it = iterable.iterator();
//              int index = -1;
//              double x = 0, y = 0;
//              int count = 0;
//              while (it.hasNext()) {
//                Tuple2<Integer, Point> p = it.next();
//                x += p.f1.x;
//                y += p.f1.y;
//                index = p.f0;
//                count++;
//              }
//              collector.collect(new Centroid(index, x / count, y / count));
//            }
//          });
//
//      // feed new centroids back into next iteration
//      DataSet<Point> finalCentroids = loop.closeWith(newPoints);
//
//      DataSet<Tuple2<Integer, Point>> clusteredPoints = points
//          // assign points to final clusters
//          .map(new KMeans.SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");
//
//      // emit result
//      if (params.has("output")) {
//        clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");
//
//        // since file sinks are lazy, we trigger the execution explicitly
//        env.execute("KMeans Example");
//      } else {
//        System.out.println("Printing result to stdout. Use --output to specify output path.");
//        clusteredPoints.print();
//      }
//    } else {
//      System.out.println("##################### Not Reduce #########################");
//      // set number of bulk iterations for KMeans algorithm
//      IterativeDataSet<Centroid> loop = centroids.iterate(params.getInt("iterations", 10));
//
//      DataSet<Centroid> newCentroids = points
//          // compute closest centroid for each point
//          .flatMap(new RichFlatMapFunction<Point, Tuple2<Integer, Point>>() {
//            private Collection<Centroid> centroids;
//            private int centroidSize;
//            private int pid;
//            private int tasks;
//            int previousCentroid = -1;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//              this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
//              this.centroidSize = centroids.size();
//              pid = getRuntimeContext().getIndexOfThisSubtask();
//              System.out.println(getRuntimeContext().getTaskNameWithSubtasks());
//              this.tasks = getRuntimeContext().getNumberOfParallelSubtasks();
//              System.out.println("%%%%%%%%%%%%%%%%%%%%   Centroid size: " + centroidSize);
//            }
//
//            @Override
//            public void flatMap(Point point, Collector<Tuple2<Integer, Point>> collector) throws Exception {
//              Random random = new Random();
//              int i =  previousCentroid == centroidSize - 1 ? 0 : previousCentroid + 1;
//              while (true) {
//                if (i % tasks == pid) {
//                  // System.out.format("Emit i=%d tasks=%d count=%d pid=%d\n", i, tasks, centroidSize, pid);
//                  previousCentroid = i;
//                  collector.collect(new Tuple2<>(i, new Point(random.nextDouble(), random.nextDouble())));
//                  break;
//                }
//                i = i + 1 == centroidSize ? 0 : i + 1;
//              }
//            }
//          }).withBroadcastSet(loop, "centroids").
//              groupBy(0).combineGroup(new RichGroupCombineFunction<Tuple2<Integer, Point>, Centroid>() {
//
//            private int pid;
//            private int tasks;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//              pid = getRuntimeContext().getIndexOfThisSubtask();
//              this.tasks = getRuntimeContext().getNumberOfParallelSubtasks();
//            }
//
//            @Override
//            public void combine(Iterable<Tuple2<Integer, Point>> iterable,
//                                Collector<Centroid> collector) throws Exception {
//              Iterator<Tuple2<Integer, Point>> it = iterable.iterator();
//              int index = -1;
//              double x = 0, y = 0;
//              int count = 0;
//              while (it.hasNext()) {
//                Tuple2<Integer, Point> p = it.next();
//                x += p.f1.x;
//                y += p.f1.y;
//                index = p.f0;
//                count++;
//              }
//              // System.out.printf("Combine pid=%d task=%d\n", pid, tasks);
//              collector.collect(new Centroid(index, x / count, y / count));
//            }
//          });
//      // feed new centroids back into next iteration
//      DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);
//
//      DataSet<Tuple2<Integer, Point>> clusteredPoints = points
//          // assign points to final clusters
//          .map(new PerfTest.SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");
//
//      // emit result
//      if (params.has("output")) {
//        clusteredPoints.writeAsCsv(params.get("output"), "\n", " ");
//
//        // since file sinks are lazy, we trigger the execution explicitly
//        env.execute("KMeans Example");
//      } else {
//        System.out.println("Printing result to stdout. Use --output to specify output path.");
//        clusteredPoints.print();
//      }
//    }
//  }
//
//  // *************************************************************************
//  //     DATA SOURCE READING (POINTS AND CENTROIDS)
//  // *************************************************************************
//  private static DataSet<Centroid> getCentroidDataSet(ParameterTool params, ExecutionEnvironment env) {
//    DataSet<Centroid> centroids;
//    if (params.has("centroids")) {
//      centroids = env.readCsvFile(params.get("centroids"))
//          .fieldDelimiter(" ")
//          .pojoType(Centroid.class, "id", "x", "y").setParallelism(params.getInt("parallel", 1));
//      ;
//    } else {
//      System.out.println("Executing K-Means example with default centroid data set.");
//      System.out.println("Use --centroids to specify file input.");
//      centroids = KMeansData.getDefaultCentroidDataSet(env, params.getInt("ccount", 100));
//    }
//    return centroids;
//  }
//
//  private static DataSet<Point> getPointDataSet(ParameterTool params, ExecutionEnvironment env) {
//    DataSet<Point> points;
//    if (params.has("points")) {
//      // read points from CSV file
//      points = env.readCsvFile(params.get("points"))
//          .fieldDelimiter(" ")
//          .pojoType(Point.class, "x", "y").setParallelism(params.getInt("parallel", 1));
//    } else {
//      System.out.println("Executing K-Means example with default point data set.");
//      System.out.println("Use --points to specify file input.");
//      points = KMeansData.getDefaultPointDataSet(env, params.getInt("pcount", 1000));
//    }
//    return points;
//  }
//}
