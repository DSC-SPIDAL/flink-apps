package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.DoubleStatistics;
import edu.iu.dsc.flink.damds.configuration.ConfigurationMgr;
import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import edu.iu.dsc.flink.damds.types.Iteration;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import java.io.File;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.io.Serializable;
import java.util.List;

public class DAMDS implements Serializable {
  private DataLoader loader;

  public DAMDSSection config;

  public ExecutionEnvironment env;

  public DAMDS(DAMDSSection config, ExecutionEnvironment env) {
    this.env = env;
    this.config = config;
    this.loader = new DataLoader(env, config);
  }

  public void setupIteration(Iteration iteration, Configuration parameters, String initialPointFile) {
    File f = new File("varray");
    f.delete();
    DataSet<Iteration> iterationDataSet = env.fromElements(iteration);
    // read the distances partitioned
    DataSet<ShortMatrixBlock> distances = loader.loadMatrixBlock();
    DataSet<ShortMatrixBlock> weights = loader.loadWeightBlock();

    DataSet<Integer> count = count(distances);
    count.writeAsText("distance_count", FileSystem.WriteMode.OVERWRITE);
    count = count(weights);
    count.writeAsText("weight_count", FileSystem.WriteMode.OVERWRITE);
    // read the distance statistics
    DataSet<DoubleStatistics> stats = Statistics.calculateStatistics(distances);
    distances = Distances.updateDistances(distances, stats);
    // now load the points
    DataSet<Matrix> prex = loader.loadInitPointDataSet(initialPointFile);
    DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> distanceWeights = Distances.calculate(distances, weights);
    // generate vArray
    DataSet<Tuple2<Matrix, ShortMatrixBlock>> vArray = VArray.generateVArray(distanceWeights, parameters);
    vArray.writeAsText("varray", FileSystem.WriteMode.OVERWRITE);
    // add tcur and tmax to matrix
    prex = joinStats(prex, stats, iterationDataSet);

    // calculate the initial stress
    DataSet<Double> preStress = Stress.calculate(distances, prex);
    DataSet<Matrix> bc = BC.calculate(prex, distanceWeights);
    bc.writeAsText("bc1.txt", FileSystem.WriteMode.OVERWRITE);
    DataSet<Matrix> newPrex = CG.calculateConjugateGradient(prex, bc, vArray, parameters, config.cgIter);
    // now calculate stress
    DataSet<Double> postStress = Stress.calculate(distances, newPrex);

    iterationDataSet = updateIteration(iterationDataSet, preStress, postStress);
    // write the iteration
    iterationDataSet.writeAsText(config.outFolder + "/" + config.iterationFile, FileSystem.WriteMode.OVERWRITE);
    // save the iteration
    newPrex.writeAsText(config.pointsFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
  }

  public DataSet<Iteration> loadInitialTemperature(Configuration parameters) {
    // read the distances partitioned
    DataSet<ShortMatrixBlock> distances = loader.loadMatrixBlock();
    // read the distance statistics
    DataSet<DoubleStatistics> stats = Statistics.calculateStatistics(distances);
    DataSet<Iteration> update = stats.map(new RichMapFunction<DoubleStatistics, Iteration>() {
      int targetDimension;
      double tMinFactor;
      double alpha;
      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.targetDimension = parameters.getInteger(Constants.TARGET_DIMENSION, 3);
        this.tMinFactor = parameters.getDouble(Constants.TMIN_FACTOR, 0.5);
        this.alpha = parameters.getDouble(Constants.ALPHA, .95);
      }

      @Override
      public Iteration map(DoubleStatistics summery) throws Exception {
        Iteration iteration = new Iteration();
        iteration.tMin = tMinFactor * summery.getPositiveMin() / Math.sqrt(2.0 * targetDimension);
        double tMax = summery.getMax() / Math.sqrt(2.0 * targetDimension);
        iteration.tCur = alpha * tMax;
        return iteration;
      }
    }).withParameters(parameters);
    return update;
  }

  public void execute() throws Exception {
    Configuration parameters = ConfigurationMgr.getConfiguration(config);
    // first load the intial temperaturs etc
    DataSet<Iteration> initialIteration = loadInitialTemperature(parameters);
    initialIteration.writeAsText(config.outFolder + "/" + config.iterationFile,
        FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    env.execute();
    // now lets load the initial iteration from file system
    Iteration iteration = loader.loadIteration();
    boolean initLoaded = false;
    String initFile;
    // first lets read the last iteration results from file system
    while (true) {
      iteration.preStress = config.threshold + 1;
      iteration.stressItr = 0;
      iteration.stress = 0;
      while (iteration.preStress - iteration.stress >= config.threshold) {
        // first we load from initial point file. then we use the previous iterations output
        if (!initLoaded) {
          initFile = config.initialPointsFile;
          initLoaded = true;
        } else {
          initFile= config.pointsFile;
        }
        setupIteration(iteration, parameters, initFile);
        env.execute();
        iteration = loader.loadIteration();
        iteration.stressItr++;
        System.out.println("************************************* Done iteration: stress=" +
            iteration.stressItr + " Temp=" + iteration.tItr);
      }

      iteration.tItr++;
      if (iteration.tCur == 0) {
        break;
      }

      iteration.tCur *= config.alpha;
      if (iteration.tCur < .01) {
        iteration.tCur = 0;
      }
    }
  }

  public DataSet<Iteration> updateIteration(DataSet<Iteration> itr, DataSet<Double> preStress,
                                            DataSet<Double> postStress) {
    DataSet<Iteration> update = itr.map(new RichMapFunction<Iteration, Iteration>() {
      @Override
      public Iteration map(Iteration iteration) throws Exception {
        List<Double> preStressList = getRuntimeContext().getBroadcastVariable("preStress");
        List<Double> postStressList = getRuntimeContext().getBroadcastVariable("postStress");

        iteration.preStress = preStressList.get(0);
        iteration.stress = postStressList.get(0);
        return iteration;
      }
    }).withBroadcastSet(preStress, "preStress").withBroadcastSet(postStress, "postStress");
    return update;
  }

  public DataSet<Boolean> stressDiff(DataSet<Double> preStree, DataSet<Double> postStress, Configuration parameters) {
    DataSet<Boolean> thresh = preStree.map(new RichMapFunction<Double, Boolean>() {
      double threshold;
      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        threshold = parameters.getDouble(Constants.THRESHOLD, 0.000001);
      }

      @Override
      public Boolean map(Double aDouble) throws Exception {
        List<Double> postStressList = getRuntimeContext().getBroadcastVariable("stat");
        double post = postStressList.get(0);
        double diffStress = aDouble - post;
        return diffStress >= threshold;
      }
    }).withBroadcastSet(postStress, "s").withParameters(parameters);
    return thresh;
  }

  public DataSet<Matrix> joinStats(DataSet<Matrix> prex, DataSet<DoubleStatistics> statisticsDataSet,
                                   DataSet<Iteration> iteration) {
    DataSet<Matrix> matrixDataSet = prex.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix matrix) throws Exception {
        List<DoubleStatistics> statList = getRuntimeContext().getBroadcastVariable("stat");
        List<Iteration> iterationList = getRuntimeContext().getBroadcastVariable("itr");
        DoubleStatistics stat = statList.get(0);
        Iteration itr = iterationList.get(0);
        matrix.getProperties().put("invs", 1.0 / stat.getSumOfSquare());
        matrix.getProperties().put("tCur", itr.tCur);
        return matrix;
      }
    }).withBroadcastSet(statisticsDataSet, "stat").withBroadcastSet(iteration, "itr");
    return matrixDataSet;
  }

  public DataSet<Double> createTCur(DataSet<DoubleStatistics> statisticsDataSet, Configuration parameters) {
    DataSet<Double> tCur = statisticsDataSet.map(new RichMapFunction<DoubleStatistics, Double>() {
      int targetDimension;
      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.targetDimension = parameters.getInteger(Constants.TARGET_DIMENSION, 3);
      }

      @Override
      public Double map(DoubleStatistics stat) throws Exception {
        return .95 * stat.getMax() / Math.sqrt(2.0 * targetDimension);
      }
    });
    return tCur;
  }

  public DataSet<Integer> count(DataSet<ShortMatrixBlock> distances) {
    DataSet<Integer> count = distances.map(new RichMapFunction<ShortMatrixBlock, Integer>() {
      @Override
      public Integer map(ShortMatrixBlock shortMatrixBlock) throws Exception {
        System.out.println("Parallel tasks for count: " + getRuntimeContext().getNumberOfParallelSubtasks());
        return 1;
      }
    }).reduce(new RichReduceFunction<Integer>() {
      @Override
      public Integer reduce(Integer integer, Integer t1) throws Exception {
        return integer + t1;
      }
    });
    return count;
  }
}
