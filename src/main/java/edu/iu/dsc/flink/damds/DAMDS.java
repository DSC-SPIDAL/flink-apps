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

  public DAMDS(DAMDSSection config) {
    env = ExecutionEnvironment.getExecutionEnvironment();
    this.config = config;
    this.loader = new DataLoader(env, config);
  }

  public void setupWorkFlow() {
    DataSet<ShortMatrixBlock> distances = loader.loadMatrixBlock();
    DataSet<Matrix> prex = loader.loadPointDataSet();

    DataSet<Double> preStress = Stress.setupWorkFlow(distances, prex);


  }

  public void execute() {
    // execute BC

    // now execute CG
  }
}
