package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.MatrixUtils;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class BC {
  public static DataSet<Matrix> calculate(DataSet<Matrix> prex, DataSet<ShortMatrixBlock> distances) {
    DataSet<Matrix> dataSet = distances.map(new RichMapFunction<ShortMatrixBlock, Tuple2<Integer, Matrix>>() {
      @Override
      public Tuple2<Integer, Matrix> map(ShortMatrixBlock sMB) throws Exception {
        System.out.println("BC calculate ************");
        List<Matrix> matrix = getRuntimeContext().getBroadcastVariable("prex");
        Matrix prexMatrix = matrix.get(0);
        double tCur = (double) prexMatrix.getProperties().get("tCur");
        double[][] internalBofZ = new double[sMB.getBlockRows()][];
        for (int i = 0; i < sMB.getBlockRows(); i++) {
          internalBofZ[i] = new double[sMB.getMatrixCols()];
        }
        double[] threadPartialBCInternalMM = new double[prexMatrix.getCols() * sMB.getBlockRows()];
        calculateBCInternal(prexMatrix.getData(), prexMatrix.getCols(), tCur, sMB.getData(), sMB.getBlockRows(),
            internalBofZ, threadPartialBCInternalMM, sMB.getBlockRows(), sMB.getStart(), sMB.getMatrixCols());

        Matrix retMatrix = new Matrix(threadPartialBCInternalMM, sMB.getBlockRows(), prexMatrix.getCols(), false);
        return new Tuple2<Integer, Matrix>(sMB.getIndex(), retMatrix);
      }
    }).withBroadcastSet(prex, "prex").reduceGroup(new GroupReduceFunction<Tuple2<Integer, Matrix>, Matrix>() {
      @Override
      public void reduce(Iterable<Tuple2<Integer, Matrix>> iterable, Collector<Matrix> collector) throws Exception {
        TreeSet<Tuple2<Integer, Matrix>> set = new TreeSet<Tuple2<Integer, Matrix>>(new Comparator<Tuple2<Integer, Matrix>>() {
          @Override
          public int compare(Tuple2<Integer, Matrix> o1, Tuple2<Integer, Matrix> o2) {
            return o1.f0.compareTo(o2.f0);
          }
        });
        // gather the reduce
        int rows = 0;
        int cols = 0;
        for (Tuple2<Integer, Matrix> t : iterable) {
          set.add(t);
          rows += t.f1.getRows();
          cols = t.f1.getCols();
        }
        int cellCount = 0;
        double[] vals = new double[rows * cols];
        for (Tuple2<Integer, Matrix> t : set) {
          System.out.printf("copy vals.size=%d rowCount=%d f1.length=%d\n", rows, cellCount, t.f1.getData().length);
          System.arraycopy(t.f1.getData(), 0, vals, cellCount, t.f1.getData().length);
          cellCount += t.f1.getData().length;
        }
        Matrix retMatrix = new Matrix(vals, rows, cols, false);
        collector.collect(retMatrix);
      }
    });
    return dataSet;
  }

  private static void calculateBCInternal(
      double[] preX, int targetDimension, double tCur,
      short[] distances, int blockSize,
      double[][] internalBofZ, double[] outMM, int blockRowCount, int rowStartIndex, int globalColCount) {

    calculateBofZ(preX, targetDimension, tCur,
        distances, internalBofZ, blockRowCount, rowStartIndex, globalColCount);

    // Next we can calculate the BofZ * preX.
    MatrixUtils.matrixMultiply(internalBofZ, preX,
        blockRowCount, targetDimension,
        globalColCount, blockSize, outMM);
  }

  private static void calculateBofZ(
      double[] preX, int targetDimension, double tCur, short[] distances,
      double[][] outBofZ, int blockRowCount, int rowStartIndex, int globalColCount) {

    double vBlockValue = -1;
    double[] outBofZLocalRow;
    double origD, weight, dist;

    double diff = 0.0;
    if (tCur > 10E-10) {
      diff = Math.sqrt(2.0 * targetDimension) * tCur;
    }

    int globalRow, procLocalRow;
    for (int localRow = 0; localRow < blockRowCount; ++localRow) {
      globalRow = localRow + rowStartIndex;
      procLocalRow = localRow;
      outBofZLocalRow = outBofZ[localRow];
      outBofZLocalRow[globalRow] = 0;
      for (int globalCol = 0; globalCol < globalColCount; globalCol++) {
        /*
				 * B_ij = - w_ij * delta_ij / d_ij(Z), if (d_ij(Z) != 0) 0,
				 * otherwise v_ij = - w_ij.
				 *
				 * Therefore, B_ij = v_ij * delta_ij / d_ij(Z). 0 (if d_ij(Z) >=
				 * small threshold) --> the actual meaning is (if d_ij(Z) == 0)
				 * BofZ[i][j] = V[i][j] * deltaMat[i][j] / CalculateDistance(ref
				 * preX, i, j);
				 */
        // this is for the i!=j case. For i==j case will be calculated
        // separately (see above).
        if (globalRow == globalCol) continue;

        origD = distances[procLocalRow * globalColCount + globalCol] * DAMDSUtils.INV_SHORT_MAX;
        weight = 1;
        if (origD < 0 || weight == 0) {
          continue;
        }
        dist = DAMDSUtils.calculateEuclideanDist(preX, globalRow, globalCol, targetDimension);
        if (dist >= 1.0E-10 && diff < origD) {
          outBofZLocalRow[globalCol] = (weight * vBlockValue * (origD - diff) / dist);
        } else {
          outBofZLocalRow[globalCol] = 0;
        }

        outBofZLocalRow[globalRow] -= outBofZLocalRow[globalCol];
      }
    }
  }


}
