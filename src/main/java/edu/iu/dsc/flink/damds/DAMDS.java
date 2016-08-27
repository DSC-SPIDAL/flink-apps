package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.DoubleStatistics;
import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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
    DataSet<ShortMatrixBlock> distances = loader.loadMatrixBlockTest();
    DataSet<DoubleStatistics> stats = Statistics.calculateStatistics(distances);
    DataSet<Matrix> prex = loader.loadPointDataSet(1, 1);
    prex = joinStats(prex, stats);

    DataSet<Double> preStress = Stress.setupWorkFlow(distances, prex);

    preStress.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);
  }

  public DataSet<Matrix> joinStats(DataSet<Matrix> prex, DataSet<DoubleStatistics> statisticsDataSet) {
    DataSet<Matrix> matrixDataSet = prex.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix matrix) throws Exception {
        List<DoubleStatistics> statList = getRuntimeContext().getBroadcastVariable("stat");
        DoubleStatistics stat = statList.get(0);
        matrix.getProperties().put("invs", 1.0/stat.getSumOfSquare());
        matrix.getProperties().put("tCur", .95 * stat.getMax() / Math.sqrt(2.0 * matrix.getCols()));
        return matrix;
      }
    }).withBroadcastSet(statisticsDataSet, "stat");
    return matrixDataSet;
  }

  public void execute() throws Exception {
    env.execute();
  }
}
