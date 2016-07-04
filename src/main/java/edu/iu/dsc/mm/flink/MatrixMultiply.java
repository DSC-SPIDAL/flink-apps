package edu.iu.dsc.mm.flink;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MatrixMultiply {
  private static final Logger LOG = LoggerFactory
      .getLogger(MatrixMultiply.class);

  private final static String outFile = "/home/supun/dev/projects/dsspidal/flink_mm/flink-mm-git/out.output";
  private final static String filePath = "/home/supun/dev/projects/dsspidal/flink_mm/flink-mm-git/out.input";

  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    final List<MatrixBlock> matrixABlocks = new ArrayList<MatrixBlock>();
    int noBlocks = 2;
    int matrixRows = 10;
    int matrixCols = 10;
    System.out.println("A");
    double []data = new double[matrixCols * matrixRows];
    int index = 0;
    for (int i = 0; i < noBlocks; i++) {
      MatrixBlock matrixABlock = new MatrixBlock();
      matrixABlock.matrixCols = matrixCols;
      matrixABlock.matrixRows = matrixRows;
      matrixABlock.blockRows = matrixRows / noBlocks;
      matrixABlock.start = i * matrixABlock.blockRows;
      matrixABlock.index = i;

      int dataSize = matrixABlock.blockRows * matrixABlock.matrixCols;
      matrixABlock.data = new double[dataSize];
      for (int d = 0; d < dataSize; d++) {
        int tmp = index;
        matrixABlock.data[d] = tmp;
        data[index++] = tmp;
      }
      matrixABlocks.add(matrixABlock);
      System.out.println(matrixABlock);
    }

    Matrix matrixB = new Matrix();
    matrixB.rows = matrixRows;
    matrixB.cols = 2;
    int matrixBdataSize = matrixB.cols * matrixRows;
    matrixB.data = new double[matrixBdataSize];
    for (int i = 0; i < matrixBdataSize; i++) {
      matrixB.data[i] = i;
    }
    System.out.println("B");
    System.out.println(matrixB.toString());

    MatrixInputFormat inputFormat = new MatrixInputFormat();
    inputFormat.setBigEndian(true);
    inputFormat.setGlobalColumnCount(matrixRows);

    File deleteFile = new File(outFile);
    if (deleteFile.exists()) {
      deleteFile.delete();
    }

    MatrixFileGenerator.writeMatrixFile(matrixRows, matrixCols, data, true, filePath);

    DataSet<MatrixBlock> blockDataSet = env.readFile(inputFormat, filePath);
    //DataSet<MatrixBlock> blockDataSet2 = env.fromCollection(matrixABlocks);
    DataSet<Matrix> matrixDataSet = env.fromElements(matrixB);


    multiply(matrixABlocks, matrixB);


    blockDataSet.map(new RichMapFunction<MatrixBlock, MatrixBlock>() {
      @Override
      public MatrixBlock map(MatrixBlock matrixABlock) throws Exception {
        List<Matrix> matrix = getRuntimeContext().getBroadcastVariable("single_matrix");
        Matrix matrixB = matrix.get(0);
        // System.out.println("Multiply: " + matrixB.toString());
        System.out.format("Multiply: (%d) = %s\n", matrixABlock.index, matrixABlock.toString());
        int cDataSize = matrixB.cols *  matrixABlock.blockRows;
        double []C = new double[cDataSize];
        Utils.matrixMultiply(matrixABlock.data, matrixB.data, matrixABlock.matrixRows, matrixABlock.matrixCols, matrixB.cols, matrixABlock.blockRows, C);

        MatrixBlock b = new MatrixBlock();
        b.data = C;
        b.index = matrixABlock.index;
        b.blockRows = matrixABlock.blockRows;
        b.matrixRows = matrixABlock.matrixRows;
        b.matrixCols = matrixB.cols;
        // start of this block calculated using the A's cols
        b.start = matrixABlock.start;

        System.out.format("After multiply: (%d) = %s\n", matrixABlock.index, b.toString());

        return b;
      }
    }).withBroadcastSet(matrixDataSet, "single_matrix").reduceGroup(new GroupReduceFunction<MatrixBlock, Matrix>() {
      @Override
      public void reduce(Iterable<MatrixBlock> iterable, Collector<Matrix> collector) throws Exception {
        Matrix m = new Matrix();
        m.setColumnMajor(false);

        boolean init = false;
        Iterator<MatrixBlock> b = iterable.iterator();
        while (b.hasNext()) {
          MatrixBlock matrixBlock = b.next();
          if (!init) {
            m.data = new double[matrixBlock.matrixCols * matrixBlock.matrixRows];
            m.rows = matrixBlock.matrixRows;
            m.cols = matrixBlock.matrixCols;
            init = true;
          }
          System.out.format("Reduce matrix index: %d = %s\n", matrixBlock.index, matrixBlock.toString());
          System.out.format("m size=(%d X %d) block is=%d block rows=%d\n", m.rows, m.cols, matrixBlock.index, matrixBlock.blockRows);
          System.out.format("index is=%d lenght is=%d\n", matrixBlock.index * matrixBlock.blockRows, matrixBlock.blockRows * matrixBlock.matrixCols);
          System.arraycopy(matrixBlock.data, 0, m.data, matrixBlock.start * m.cols, matrixBlock.blockRows * matrixBlock.matrixCols);
        }
        collector.collect(m);
      }
    }).writeAsText(outFile);

    env.execute();
  }

  private static void multiply(List<MatrixBlock> matrixABlocks, Matrix matrixB) {
    for (int i = 0; i < matrixABlocks.size(); i++) {
      MatrixBlock matrixABlock = matrixABlocks.get(i);
      int cDataSize = matrixB.cols * matrixABlock.blockRows;
      double[] C = new double[cDataSize];
      Utils.matrixMultiply(matrixABlock.data, matrixB.data, matrixABlock.matrixRows, matrixABlock.matrixCols, matrixB.cols, matrixABlock.blockRows, C);

      Matrix cBlock = new Matrix(matrixABlock.blockRows, matrixB.cols);
      cBlock.data = C;
      System.out.println(cBlock);
    }
  }
}
