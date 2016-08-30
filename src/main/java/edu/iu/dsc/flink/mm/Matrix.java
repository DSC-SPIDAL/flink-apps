package edu.iu.dsc.flink.mm;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A matrix represented as an array. The matrix is represented in the column major format.
 */
public class Matrix implements Serializable {
  double []data;
  // now of rows
  int rows;
  // cols should be small
  int cols;

  int index;

  private Map<String, Object> properties = new HashMap<String, Object>();

  boolean columnMajor = true;

  public Matrix() {
  }

  public Matrix(double[] data, int rows, int cols, int index, boolean columnMajor) {
    this.data = data;
    this.rows = rows;
    this.cols = cols;
    this.index = index;
    this.columnMajor = columnMajor;
  }

  public Matrix(double[] data, int rows, int cols, boolean columnMajor) {
    this.data = data;
    this.rows = rows;
    this.cols = cols;
    this.columnMajor = columnMajor;
  }

  public Matrix(int rows, int cols) {
    this.rows = rows;
    this.cols = cols;
    this.data = new double[rows * cols];
  }

  public Matrix(int rows, int cols, boolean columnMajor) {
    this.rows = rows;
    this.cols = cols;
    this.columnMajor = columnMajor;
  }

  public boolean isColumnMajor() {
    return columnMajor;
  }

  public void setColumnMajor(boolean columnMajor) {
    this.columnMajor = columnMajor;
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

  public void addProperty(String prop, Object val) {
    properties.put(prop, val);
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("");
    if (columnMajor) {
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          sb.append(data[i + rows * j]).append(" ");
        }
        sb.append("\n");
      }
    } else {
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          sb.append(data[i * cols + j]).append(" ");
        }
        sb.append("\n");
      }
    }
    return sb.toString();
  }
}
