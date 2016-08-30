package edu.iu.dsc.flink.damds;

import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import edu.iu.dsc.flink.mm.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataLoader {
  private final ExecutionEnvironment env;

  private DAMDSSection config;

  public DataLoader(ExecutionEnvironment env, DAMDSSection config) {
    this.env = env;
    this.config = config;
  }

  public DataSet<ShortMatrixBlock> loadMatrixBlock() {
    ShortMatrixInputFormat inputFormat = new ShortMatrixInputFormat();
    inputFormat.setBigEndian(true);
    inputFormat.setGlobalColumnCount(config.numberDataPoints);
    inputFormat.setGlobalRowCount(config.numberDataPoints);

    return env.readFile(inputFormat, config.distanceMatrixFile);
  }

  public DataSet<ShortMatrixBlock> loadMatrixBlockTest() {
    List<ShortMatrixBlock> matrixBlockList = new ArrayList<ShortMatrixBlock>();
    int blocks = 2;
    int pointPerBlock = config.numberDataPoints / blocks;
    for (int j = 0; j < blocks; j++) {
      ShortMatrixBlock matrixB = new ShortMatrixBlock();
      matrixB.setIndex(j);
      matrixB.setMatrixCols(config.numberDataPoints);
      matrixB.setMatrixRows(pointPerBlock);
      matrixB.setStart(j * pointPerBlock);
      matrixB.setBlockRows(pointPerBlock);

      int matrixBdataSize = matrixB.getMatrixCols() * matrixB.getBlockRows();
      short[] data = new short[matrixBdataSize];
      matrixB.setData(data);
      for (short i = 0; i < matrixBdataSize; i++) {
        data[i] = (short) (i * 100);
      }
      matrixBlockList.add(matrixB);
    }
    return env.fromCollection(matrixBlockList);
  }

  public DataSet<Matrix> loadPointDataSet(double tCur, double invs) {
    Matrix matrixB = new Matrix(config.numberDataPoints, config.targetDimension);
    int matrixBdataSize = matrixB.getCols() * matrixB.getRows();
    double[] data = new double[matrixBdataSize];
    matrixB.setData(data);
    matrixB.getProperties().put("tCur", tCur);
    matrixB.getProperties().put("invs", invs);
    Random random = new Random();
    for (int i = 0; i < matrixBdataSize; i++) {
      data[i] = random.nextDouble();
    }
    return env.fromElements(matrixB);
  }

  public DataSet<Integer> loadParallelArray(int parallel) {
    List<Integer> array = new ArrayList<>();
    for (int i = 0; i < parallel; i++) {
      array.add(i);
    }
    return env.fromCollection(array);
  }
}
