package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.DoubleStatistics;
import edu.iu.dsc.flink.damds.configuration.ConfigurationMgr;
import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.common.functions.RichMapFunction;
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

  public void setupWorkFlow() {
    Configuration parameters = ConfigurationMgr.getConfiguration(config);

    // read the distances partitioned
    DataSet<ShortMatrixBlock> distances = loader.loadMatrixBlock();
    DataSet<ShortMatrixBlock> weights = loader.loadWeightBlock();
    // read the distance statistics
    DataSet<DoubleStatistics> stats = Statistics.calculateStatistics(distances);
    distances = Distances.updateDistances(distances, stats);
    // now load the points
    DataSet<Matrix> prex = loader.loadInitPointDataSet();
    DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> distanceWeights = Distances.calculate(distances, weights);
    // generate vArray
    DataSet<Tuple2<Matrix, ShortMatrixBlock>> vArray = VArray.generateVArray(distanceWeights, parameters);
    vArray.writeAsText("varray", FileSystem.WriteMode.OVERWRITE);
    // add tcur and tmax to matrix
    prex = joinStats(prex, stats);

    // calculate the initial stress
    DataSet<Double> preStress = Stress.setupWorkFlow(distances, prex);
    DataSet<Matrix> bc = BC.calculate(prex, distanceWeights);
    bc.writeAsText("bc1.txt", FileSystem.WriteMode.OVERWRITE);
    DataSet<Matrix> newPrex = CG.calculateConjugateGradient(prex, bc, vArray, parameters, config.cgIter);
    // now calculate stress
    DataSet<Double> diffStress = Stress.setupWorkFlow(distances, newPrex);
    DataSet<Boolean> terminate = streeDiff(preStress, diffStress, parameters);

    newPrex.writeAsText(config.pointsFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
  }

  public DataSet<Boolean> streeDiff(DataSet<Double> preStree, DataSet<Double> postStress, Configuration parameters) {
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

  public DataSet<Matrix> joinStats(DataSet<Matrix> prex, DataSet<DoubleStatistics> statisticsDataSet) {
    DataSet<Matrix> matrixDataSet = prex.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix matrix) throws Exception {
        List<DoubleStatistics> statList = getRuntimeContext().getBroadcastVariable("stat");
        DoubleStatistics stat = statList.get(0);
        matrix.getProperties().put("invs", 1.0 / stat.getSumOfSquare());
        matrix.getProperties().put("tCur", .95 * stat.getMax() / Math.sqrt(2.0 * matrix.getCols()));
        return matrix;
      }
    }).withBroadcastSet(statisticsDataSet, "stat");
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

  public void execute() throws Exception {
    env.execute();
  }
}
