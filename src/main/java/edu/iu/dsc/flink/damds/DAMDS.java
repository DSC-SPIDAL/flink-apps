package edu.iu.dsc.flink.damds;

import com.sun.tools.internal.jxc.ap.Const;
import edu.indiana.soic.spidal.common.DoubleStatistics;
import edu.iu.dsc.flink.damds.configuration.ConfigurationMgr;
import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import edu.iu.dsc.flink.mm.DoubleMatrixBlock;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import java.util.List;


public class DAMDS {
  private final DataLoader loader;

  public final DAMDSSection config;

  public final ExecutionEnvironment env;

  public DAMDS(DAMDSSection config, ExecutionEnvironment env) {
    this.env = env;
    this.config = config;
    this.loader = new DataLoader(env, config);
  }

  public void setupWorkFlow() {
    String filePath = "out.txt";
    Configuration parameters = ConfigurationMgr.getConfiguration(config);

    // read the distances partitioned
    DataSet<ShortMatrixBlock> distances = loader.loadMatrixBlockTest();
    // read the distance statistics
    DataSet<DoubleStatistics> stats = Statistics.calculateStatistics(distances);
    // now load the points
    DataSet<Matrix> prex = loader.loadPointDataSet();
    // generate vArray
    DataSet<Matrix> vArray = VArray.generateVArray(distances, parameters);
    // add tcur and tmax to matrix
    prex = joinStats(prex, stats);
    // calculate the initial stress

    // now create tCur, this will be our loop variable
    DataSet<Double> tCur = createTCur(stats, parameters);

    // we need to register a filter to terminate the loop
    IterativeDataSet<Double> tempLoop = tCur.iterate(config.maxtemploops);
    DataSet<Double> preStress = Stress.setupWorkFlow(distances, prex);
    // now inside this stress loop
    DataSet<Double> diffStress = env.fromElements(new Double(config.threshold + 1.0));

    IterativeDataSet<Double> stressLoop = diffStress.iterate(config.maxtemploops);
    DataSet<Matrix> bc = BC.calculate(prex, distances);
    DataSet<Matrix> newPrex = CG.calculateConjugateGradient(prex, bc, vArray, parameters, config.cgIter);

    // now calculate stress
    diffStress = Stress.setupWorkFlow(distances, newPrex);
    stressLoop.closeWith(diffStress);
    // todo close temperature loop
    tempLoop.closeWith(tCur);

    bc.writeAsText("bc.txt", FileSystem.WriteMode.OVERWRITE);
    vArray.writeAsText("vArray.txt", FileSystem.WriteMode.OVERWRITE);
    preStress.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
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
