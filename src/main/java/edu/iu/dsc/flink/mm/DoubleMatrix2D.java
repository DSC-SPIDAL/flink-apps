package edu.iu.dsc.flink.mm;

public class DoubleMatrix2D extends MatrixBlock {
  double [][]data;

  public DoubleMatrix2D() {
  }

  public DoubleMatrix2D(int index, int start, int blockRows, int matrixCols, int matrixRows, double[][] data) {
    super(index, start, blockRows, matrixCols, matrixRows);
    this.data = data;
  }

  public double[][] getData() {
    return data;
  }

  public void setData(double[][] data) {
    this.data = data;
  }
}
