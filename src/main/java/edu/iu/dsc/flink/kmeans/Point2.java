package edu.iu.dsc.flink.kmeans;

import java.io.Serializable;

public class Point2 implements Serializable {
  public double[]values;

  public Point2(double[] values) {
    this.values = values;
  }

  public Point2 add(Point2 other) {
    for (int i = 0; i < values.length; i++) {
      values[i] = other.values[i];
    }
    return this;
  }

  public Point2 div(long val) {
    for (int i = 0; i < values.length; i++) {
      values[i] = values[i] / val;
    }
    return this;
  }

  public double euclideanDistance(Point2 other) {
    double sum = 0;
    for (int i = 0; i < values.length; i++) {
      sum += (values[i] - other.values[i]) * (values[i] - other.values[i]);
    }
    return Math.sqrt(sum);
  }

  public void clear() {
    for (int i = 0; i < values.length; i++) {
      values[i] = 0;
    }
  }

  @Override
  public String toString() {
    String s = "";
    for (int i = 0; i < values.length; i++) {
      s += values[i] + " ";
    }
    return s;
  }
}
