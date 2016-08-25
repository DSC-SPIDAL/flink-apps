package edu.iu.dsc.flink.damds;

import net.openhft.lang.io.Bytes;

import java.util.Arrays;

public class DAMDSUtils {
  public static final double INV_SHORT_MAX = 1.0 / Short.MAX_VALUE;

  public static double calculateEuclideanDist(double[] v, int i, int j, int d) {
    double t = 0.0;
    double e;
    i = d * i;
    j = d * j;
    for (int k = 0; k < d; ++k) {
      e = v[i + k] - v[j + k];
      t += e * e;
    }
    return Math.sqrt(t);
  }

  public static void mergePartials(double[][] partials, double[] result){
    int offset = 0;
    for (double [] partial : partials){
      System.arraycopy(partial, 0, result, offset, partial.length);
      offset+=partial.length;
    }
  }

  public static void zeroOutArray(double[][] a){
    Arrays.fill(a[0], 0.0d);
  }

  public static double innerProductCalculation(double[] a) {
    double sum = 0.0;
    if (a.length > 0) {
      for (double anA : a) {
        sum += anA * anA;
      }
    }
    return sum;
  }

  public static double innerProductCalculation(double[] a, double[] b) {
    double sum = 0;
    if (a.length > 0) {
      for (int i = 0; i < a.length; ++i) {
        sum += a[i] * b[i];
      }
    }
    return sum;
  }

  public static void extractPoints(
      Bytes bytes, int numPoints, int dimension, double[] to) {
    int pos = 0;
    int offset;
    for (int i = 0; i < numPoints; ++i){
      offset = i*dimension;
      for (int j = 0; j < dimension; ++j) {
        bytes.position(pos);
        to[offset+j] = bytes.readDouble(pos);
        pos += Double.BYTES;
      }
    }
  }
}
