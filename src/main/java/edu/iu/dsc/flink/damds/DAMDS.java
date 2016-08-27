package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.DoubleStatistics;
import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.File;

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
    File f = new File(filePath);
    if (f.exists()) {
      f.delete();
    }
    DataSet<ShortMatrixBlock> distances = loader.loadMatrixBlockTest();
    DataSet<DoubleStatistics> stats = Statistics.calculateStatistics(distances);
    stats.writeAsText("stats.txt");

    DataSet<Matrix> prex = loader.loadPointDataSet(1, 1);

    DataSet<Double> preStress = Stress.setupWorkFlow(distances, prex);
    preStress.writeAsText(filePath);
  }

  public void execute() throws Exception {
    env.execute();
  }
}
