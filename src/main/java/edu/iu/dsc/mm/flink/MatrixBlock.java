package edu.iu.dsc.mm.flink;

import java.io.Serializable;

/**
 * Matrix block represented as an array. The matrix is represented in row major format
 */
public class MatrixBlock implements Serializable {
  // the actual data for this block
  double []data;
  // total number of rows in the matrix
  int matrixRows;
  // total number of cols in the matrxi
  int matrixCols;
  // no of rows in this block
  int blockRows;
  // start of the block
  int start;
  // block index starting from 0
  int index;

  public MatrixBlock() {
  }

  public MatrixBlock(int index, int start, int blockRows, int matrixCols, int matrixRows) {
    this.index = index;
    this.start = start;
    this.blockRows = blockRows;
    this.matrixCols = matrixCols;
    this.matrixRows = matrixRows;
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
