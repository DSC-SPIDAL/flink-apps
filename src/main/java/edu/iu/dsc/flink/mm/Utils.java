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
    // second matrix cols
    for (int k = 0; k < D; k++) {
      // first matrix rows
      for (int i = 0; i < rowBlockSize; i++) {
        // first matrix rows and second matrix cols
        sum = 0;
        for (int j = 0; j < M; j++) {
          //System.out.println("A index: " + (i * M + j));
          //System.out.format("B index: %d (k=%d, N=%d, j=%d, i=%d)\n", (k * N + j), k, N, j, i);
          sum += A[i * M + j] * B[k * N + j];
        }
        C[i * D + k] = sum;
      }
    }
  }


  public static void main(String[] args) {
    double[] A1 = new double[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 11};
    double[] A2 = new double[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    double[] B = new double[] {1, 1, 1, 1, 2, 1, 1, 1, 1, 1};

    double[] C1 = new double[4];
    double[] C2 = new double[4];

    matrixMultiply(A1, B, 4, 5, 2, 2, C1);
    matrixMultiply(A2, B, 4, 5, 2, 2, C2);

    printM(C1);
    printM(C2);
  }

  private static void printM(double[] c1) {
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        System.out.format("%f ", c1[i * 2 + j]);
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
}
