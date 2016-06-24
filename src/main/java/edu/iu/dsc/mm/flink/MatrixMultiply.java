package edu.iu.dsc.mm.flink;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MatrixMultiply {
  /**
   * Matrix block represented as an array. The matrix is represented in row major format
   */
  public static class MatrixBlock implements Serializable {
    double []data;
    int matrixRows;
    int matrixCols;
    int blockRows;
    // start of the block
    int start;
    int index;

    public MatrixBlock() {
    }

    public void setData(double[] data) {
      this.data = data;
    }

    public void setMatrixRows(int matrixRows) {
      this.matrixRows = matrixRows;
    }

    public void setMatrixCols(int matrixCols) {
      this.matrixCols = matrixCols;
    }

    public void setBlockRows(int blockRows) {
      this.blockRows = blockRows;
    }

    public void setStart(int start) {
      this.start = start;
    }

    public void setIndex(int index) {
      this.index = index;
    }

    public double[] getData() {
      return data;
    }

    public int getMatrixRows() {
      return matrixRows;
    }

    public int getMatrixCols() {
      return matrixCols;
    }

    public int getBlockRows() {
      return blockRows;
    }

    public int getStart() {
      return start;
    }

    public int getIndex() {
      return index;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < blockRows; i++) {
        for (int j = 0; j < matrixCols; j++) {
          sb.append(data[i * matrixCols + j]).append(" ");
        }
        sb.append("\n");
      }
      return sb.toString();
    }
  }

  /**
   * A matrix represented as an array. The matrix is represented in the column major format.
   */
  public static class Matrix implements Serializable {
    double []data;
    // now of rows
    int rows;
    // cols should be small
    int cols;

    public Matrix() {
    }

    public Matrix(int rows, int cols) {
      this.rows = rows;
      this.cols = cols;
      this.data = new double[rows * cols];
    }

    public double[] getData() {
      return data;
    }

    public int getRows() {
      return rows;
    }

    public int getCols() {
      return cols;
    }

    public void setData(double[] data) {
      this.data = data;
    }

    public void setRows(int rows) {
      this.rows = rows;
    }

    public void setCols(int cols) {
      this.cols = cols;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("");
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          sb.append(data[i + rows * j]).append(" ");
        }
        sb.append("\n");
      }
      return sb.toString();
    }
  }

  public static void main(String[] args) throws Exception {
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    final List<MatrixBlock> matrixABlocks = new ArrayList<MatrixBlock>();
    int noBlocks = 2;
    int matrixRows = 4;
    int matrixCols = 4;
    System.out.println("A");
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
        matrixABlock.data[d] = d;
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
    System.out.println(matrixB);

    DataSet<MatrixBlock> blockDataSet = env.fromCollection(matrixABlocks);
    DataSet<Matrix> matrixDataSet = env.fromElements(matrixB);


    multiply(matrixABlocks, matrixB);

    blockDataSet.map(new RichMapFunction<MatrixBlock, MatrixBlock>() {
      @Override
      public MatrixBlock map(MatrixBlock matrixABlock) throws Exception {
        List<Matrix> matrix = getRuntimeContext().getBroadcastVariable("single_matrix");
        Matrix matrixB = matrix.get(0);
        System.out.println("Multiply: " + matrixB.toString());
        int cDataSize = matrixB.cols *  matrixABlock.blockRows;
        double []C = new double[cDataSize];
        Utils.matrixMultiply(matrixABlock.data, matrixB.data, matrixABlock.matrixRows, matrixABlock.matrixCols, matrixB.cols, matrixABlock.blockRows, C);

        MatrixBlock b = new MatrixBlock();
        b.data = C;
        b.index = matrixABlock.index;
        b.blockRows = matrixABlock.blockRows;
        b.matrixRows = matrixABlock.matrixRows;
        b.matrixCols = matrixB.cols;
        return b;
      }
    }).withBroadcastSet(matrixDataSet, "single_matrix").reduceGroup(new GroupReduceFunction<MatrixBlock, Matrix>() {
      @Override
      public void reduce(Iterable<MatrixBlock> iterable, Collector<Matrix> collector) throws Exception {
        Matrix m = new Matrix();
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
          System.out.format("m size=(%d X %d) block is=%d block rows=%d\n", m.rows, m.cols, matrixBlock.index, matrixBlock.blockRows);
          System.out.format("index is=%d lenght is=%d\n", matrixBlock.index * matrixBlock.blockRows, matrixBlock.blockRows * matrixBlock.matrixCols);
          System.arraycopy(matrixBlock.data, 0, m.data, matrixBlock.index * matrixBlock.blockRows * matrixBlock.matrixCols, matrixBlock.blockRows * matrixBlock.matrixCols);
        }
        collector.collect(m);
      }
    }).writeAsText("/home/supun/deploy/flink/flink-1.0.3/out.txt");

    env.execute();
  }

  public static void multiply(List<MatrixBlock> matrixABlocks, Matrix matrixB) {
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
