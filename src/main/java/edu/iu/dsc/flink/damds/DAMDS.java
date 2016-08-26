package edu.iu.dsc.flink.damds;

import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

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
    DataSet<ShortMatrixBlock> distances = loader.loadMatrixBlockTest();
    DataSet<Matrix> prex = loader.loadPointDataSet();

    DataSet<Double> preStress = Stress.setupWorkFlow(distances, prex);
    preStress.writeAsText("out.txt");
  }

  public void execute() throws Exception {
    env.execute();
  }
}
