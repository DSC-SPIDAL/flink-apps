package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.MatrixUtils;
import edu.indiana.soic.spidal.common.WeightsWrap1D;
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
  public static DataSet<Matrix> calculate(DataSet<Matrix> prex,
                                          DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> distancesWeights) {
    DataSet<Matrix> dataSet = distancesWeights.map(new RichMapFunction<Tuple2<ShortMatrixBlock, ShortMatrixBlock>, Tuple2<Integer, Matrix>>() {
      @Override
      public Tuple2<Integer, Matrix> map(Tuple2<ShortMatrixBlock, ShortMatrixBlock> tuple) throws Exception {
        //System.out.println("BC calculate ************");
        List<Matrix> matrix = getRuntimeContext().getBroadcastVariable("prex");
        ShortMatrixBlock distanceBlock = tuple.f0;
        ShortMatrixBlock weightBlock = tuple.f1;
        Matrix prexMatrix = matrix.get(0);
        double tCur = (double) prexMatrix.getProperties().get("tCur");
        double[][] internalBofZ = new double[distanceBlock.getBlockRows()][];
        for (int i = 0; i < distanceBlock.getBlockRows(); i++) {
          internalBofZ[i] = new double[distanceBlock.getMatrixCols()];
        }
        double[] threadPartialBCInternalMM = new double[prexMatrix.getCols() * distanceBlock.getBlockRows()];
        WeightsWrap1D weights = new WeightsWrap1D(weightBlock.getData(), null, false, weightBlock.getMatrixCols());
        calculateBCInternal(prexMatrix.getData(), prexMatrix.getCols(), tCur, distanceBlock.getData(), distanceBlock.getBlockRows(),
            internalBofZ, threadPartialBCInternalMM, distanceBlock.getBlockRows(), distanceBlock.getStart(), distanceBlock.getMatrixCols(), weights);

        Matrix retMatrix = new Matrix(threadPartialBCInternalMM, distanceBlock.getBlockRows(), prexMatrix.getCols(), false);
        return new Tuple2<Integer, Matrix>(distanceBlock.getIndex(), retMatrix);
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
          //System.out.printf("copy vals.size=%d rowCount=%d f1.length=%d\n", rows, cellCount, t.f1.getData().length);
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
      double[][] internalBofZ, double[] outMM, int blockRowCount, int rowStartIndex, int globalColCount, WeightsWrap1D weightsWrap1D) {

    calculateBofZ(preX, targetDimension, tCur,
        distances, internalBofZ, blockRowCount, rowStartIndex, globalColCount, weightsWrap1D);

    // Next we can calculate the BofZ * preX.
    MatrixUtils.matrixMultiply(internalBofZ, preX,
        blockRowCount, targetDimension,
        globalColCount, blockSize, outMM);
  }

  private static void calculateBofZ(
      double[] preX, int targetDimension, double tCur, short[] distances,
      double[][] outBofZ, int blockRowCount, int rowStartIndex, int globalColCount, WeightsWrap1D weights) {

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
        weight = weights.getWeight(procLocalRow, globalCol);
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
