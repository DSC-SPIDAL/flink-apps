package edu.iu.dsc.flink.mm;

import java.io.Serializable;

/**
 * Matrix block represented as an array. The matrix is represented in row major format
 */
public abstract class MatrixBlock implements Serializable {
  // total number of rows in the matrix
  protected int matrixRows;
  // total number of cols in the matrxi
  protected int matrixCols;
  // no of rows in this block
  protected int blockRows;
  // start of the block
  protected int start;
  // block index starting from 0
  protected int index;

  public MatrixBlock() {
  }

  public MatrixBlock(int index, int start, int blockRows, int matrixCols, int matrixRows) {
    this.index = index;
    this.start = start;
    this.blockRows = blockRows;
    this.matrixCols = matrixCols;
    this.matrixRows = matrixRows;
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
}
