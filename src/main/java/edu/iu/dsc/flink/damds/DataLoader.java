package edu.iu.dsc.flink.damds;

import com.google.common.base.Strings;
import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import edu.iu.dsc.flink.mm.*;
import mpi.MPIException;
import net.openhft.lang.io.Bytes;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.regex.Pattern;

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

  public DataSet<Matrix> loadInitPointDataSet() {
    int n = config.numberDataPoints;
    int m = config.targetDimension;
    Matrix matrixB = new Matrix(n, m);
    Path path = Paths.get(config.initialPointsFile);
    try {
      try (Scanner scanner = new Scanner(path)) {
        Pattern pattern = Pattern.compile("[\t]");
        double[] preX = new double[n * m];
        int row = 0;
        while (scanner.hasNextLine()) {
          String line = scanner.nextLine();
          //process each line in some way
          if (Strings.isNullOrEmpty(line))
            continue; // continue on empty lines - "while" will break on null anyway;

          String[] splits = pattern.split(line.trim());
          for (int i = 0; i < m; ++i) {
            preX[row + i] = Double.parseDouble(splits[i].trim());
          }
          row += m;
        }
        matrixB.setData(preX);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read file", e);
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
