package edu.iu.dsc.mm.flink;

import java.io.Serializable;

/**
 * A matrix represented as an array. The matrix is represented in the column major format.
 */
public class Matrix implements Serializable {
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
