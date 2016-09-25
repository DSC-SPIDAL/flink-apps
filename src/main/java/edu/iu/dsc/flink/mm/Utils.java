package edu.iu.dsc.flink.mm;

import com.google.common.base.Optional;
import org.apache.commons.cli.*;

public class Utils {
  /**
   *
   * @param A Row matrix form, A is a NxM matrix, A has the blocks from start to start + size
   * @param B Column matrix representation
   * @param N
   * @param M
   * @param D
   * @param C
   */
  public static void matrixMultiply(double[] A, double[] B, int N, int M, int D,
                                    int rowBlockSize, double[] C) {
    double sum;
    for (int i = 0; i <rowBlockSize; i++) {
      for (int j = 0; j < D; j++) {
        sum = 0;
        for (int k = 0; k < M; k++) {
          //System.out.println("i * M + k = " + (i * M + k) );
          //System.out.println("j * M + k = " + (i * M + k) );
         // System.out.println((i * M + k));
          //System.out.println((j * M + k));
          //System.out.printf("i=%d M=%d k=%d j=%d D=%d blockSize=%d\n", i, M, k, j, D, rowBlockSize);
          //System.out.printf("A[i * M + k]=%f and B[j * M + k]=%f and i=%d M=%d k=%d j=%d\n", A[i * M + k], B[j * M + k], i, M, k, j);
          sum += A[i * M + k] * B[j * M + k];
        }
//        System.out.format("(i * M + k)=%d i=%d M=%d j=%d sum=%f\n", (i * M + j), i, M, j, sum);
        C[i * D + j] = sum;
      }
    }
  }


  public static void main(String[] args) {
    double[] A1 = new double[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 11};
    double[] A2 = new double[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    double[] B = new double[] {1, 1, 1, 1, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};
    System.out.println("A");
    printM(A1, 2, 5);
    System.out.println("B");
    printMColMajor(B, 5, 3);

    double[] C1 = new double[6];
    double[] C2 = new double[6];

    matrixMultiply(A1, B, 4, 5, 3, 2, C1);
    // matrixMultiply(A2, B, 4, 5, 3, 2, C2);

    printM(C1, 2, 3);
    // printM(C2, 2, 3);
  }

  private static void printMColMajor(double[] c1, int rows, int cols) {
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        System.out.format("%f ", c1[j * rows + i]);
      }
      System.out.format("\n");
    }
  }

  private static void printM(double[] c1, int rows, int cols) {
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        System.out.format("%f ", c1[i * cols + j]);
      }
      System.out.format("\n");
    }
  }

  /**
   * Parse command line arguments
   *
   * @param args Command line arguments
   * @param opts Command line options
   * @return An <code>Optional&lt;CommandLine&gt;</code> object
   */
  public static Optional<CommandLine> parseCommandLineArguments(
      String[] args, Options opts) {
    CommandLineParser optParser = new GnuParser();
    try {
      return Optional.fromNullable(optParser.parse(opts, args));
    } catch (ParseException e) {
      System.out.println(e);
    }
    return Optional.fromNullable(null);
  }

  public static Option createOption(String opt, boolean hasArg, String description, boolean required) {
    Option symbolListOption = new Option(opt, hasArg, description);
    symbolListOption.setRequired(required);
    return symbolListOption;
  }
}
