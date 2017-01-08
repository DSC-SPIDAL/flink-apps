package edu.iu.dsc.flink.kmeans;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.core.fs.FileSystem;

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
 *
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>Bulk iterations
 * <li>Broadcast variables in bulk iterations
 * <li>Custom Java objects (POJOs)
 * </ul>
 */
@SuppressWarnings("serial")
public class KMeansOriginal {

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

        // get input data:
        // read the points and centroids from the provided paths or fall back to default data
        DataSet<Point2> points;
        DataSet<String> pointLines = getDataSet(params.get("points"), env);
        DataSet<String> centroidLines = getDataSet(params.get("centroids"), env);

        DataSet<Centroid2> centroids;
        DataSet<Integer> centroidIndexes = getCentroidIds(params, env);

        points = pointLines.map(new RichMapFunction<String, Point2>() {
            @Override
            public Point2 map(String s) throws Exception {
                String[] split = s.split(" ");
                double[] values = new double[split.length];
                for (int i = 0; i < split.length; i++) {
                    values[i] = Double.parseDouble(split[i]);
                }
                return new Point2(values);
            }
        });

        centroids = centroidLines.map(new RichMapFunction<String, Centroid2>() {
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
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                        // count and sum point coordinates for each centroid
                .map(new CountAppender())
                .groupBy(0).reduce(new CentroidAccumulator())
                        // compute new centroids from point counts and coordinate sums
                .map(new CentroidAverager());

        DataSet<Centroid2> filledCentroids = centroidIndexes.map(new RichMapFunction<Integer, Centroid2>() {
            int k = 0;
            @Override
            public Centroid2 map(Integer integer) throws Exception {
                double []zeros;
                zeros = new double[k];
                Centroid2 fill = new Centroid2(integer, zeros);
                for (Centroid2 c : centroids) {
                    if (c.id == integer) {
                        return c;
                    }
                }
                System.out.println(String.format("%d not in the set", integer));
                return fill;
            }

            List<Centroid2> centroids;
            @Override
            public void open(Configuration parameters) throws Exception {
                this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
                k = parameters.getInteger("k", 100);

            }
        }).withBroadcastSet(newCentroids, "centroids");
        DataSet<Centroid2> finalCentroids = loop.closeWith(filledCentroids);

        // emit result
        if (params.has("output")) {
            finalCentroids.writeAsText(params.get("output"), FileSystem.WriteMode.OVERWRITE);

            // since file sinks are lazy, we trigger the execution explicitly'
            // env.setParallelism(1);
            env.execute("KMeans Example");
            System.out.println(String.format("Total time: %d", env.getLastJobExecutionResult().getNetRuntime()));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            finalCentroids.print();
        }
    }

    private static DataSet<Integer> getCentroidIds(ParameterTool params, ExecutionEnvironment env) {
        List<Integer> indexes = new ArrayList<>();
        int k = params.getInt("k");
        for (int i = 0; i < k; i++) {
            indexes.add(i + 1);
        }
        return env.fromCollection(indexes);
    }

    private static DataSet<String> getDataSet(String file, ExecutionEnvironment env) {
        DataSet<String> points = null;
        points = env.readTextFile(file);
        return points;
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Determines the closest cluster center for a data point. */
    //@ForwardedFields("*->1")
    public static final class SelectNearestCenter extends RichMapFunction<Point2, Tuple2<Integer, Point2>> {
        private Collection<Centroid2> centroids;

        /** Reads the centroid values from a broadcast variable into a collection. */
        @Override
        public void open(Configuration parameters) throws Exception {
            this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
        }

        @Override
        public Tuple2<Integer, Point2> map(Point2 p) throws Exception {

            double minDistance = Double.MAX_VALUE;
            int closestCentroidId = -1;

            // check all cluster centers
            for (Centroid2 centroid : centroids) {
                // compute distance
                double distance = p.euclideanDistance(centroid);

                // update nearest cluster if necessary
                if (distance < minDistance) {
                    minDistance = distance;
                    closestCentroidId = centroid.id;
                }
            }
            // System.out.println(String.format("Emitting %d ", closestCentroidId));
            // emit a new record with the center id and the data point.
            return new Tuple2<>(closestCentroidId, p);
        }
    }

    /** Appends a count variable to the tuple. */
   // @ForwardedFields("f0;f1")
    public static final class CountAppender implements MapFunction<Tuple2<Integer, Point2>, Tuple3<Integer, Point2, Long>> {

        @Override
        public Tuple3<Integer, Point2, Long> map(Tuple2<Integer, Point2> t) {
            return new Tuple3<>(t.f0, t.f1, 1L);
        }
    }

    /** Sums and counts point coordinates. */
    //@ForwardedFields("0")
    public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point2, Long>> {

        @Override
        public Tuple3<Integer, Point2, Long> reduce(Tuple3<Integer, Point2, Long> val1, Tuple3<Integer, Point2, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
        }
    }

    /** Computes new centroid from coordinate sum and count of points. */
    //@ForwardedFields("0->id")
    public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point2, Long>, Centroid2> {

        @Override
        public Centroid2 map(Tuple3<Integer, Point2, Long> value) {
            return new Centroid2(value.f0, value.f1.div(value.f2));
        }
    }
}
