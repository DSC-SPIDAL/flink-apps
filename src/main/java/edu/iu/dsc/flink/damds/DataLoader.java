package edu.iu.dsc.flink.damds;

import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.MatrixBlock;
import edu.iu.dsc.flink.mm.MatrixInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.Random;

public class DataLoader {
  private final ExecutionEnvironment env;

  private DAMDSSection config;

  public DataLoader(ExecutionEnvironment env, DAMDSSection config) {
    this.env = env;
    this.config = config;
  }

  public DataSet<MatrixBlock> loadMatrixBlock() {
    MatrixInputFormat inputFormat = new MatrixInputFormat();
    inputFormat.setBigEndian(true);
    inputFormat.setGlobalColumnCount(config.numberDataPoints);
    inputFormat.setGlobalRowCount(config.numberDataPoints);

    return env.readFile(inputFormat, config.distanceMatrixFile);
  }

  public DataSet<Matrix> loadPointDataSet() {
    Matrix matrixB = new Matrix(config.numberDataPoints, config.targetDimension);
    int matrixBdataSize = matrixB.getCols() * matrixB.getRows();
    double[] data = new double[matrixBdataSize];
    matrixB.setData(data);
    Random random = new Random();
    for (int i = 0; i < matrixBdataSize; i++) {
      data[i] = random.nextDouble();
    }
    return env.fromElements(matrixB);
  }
}
