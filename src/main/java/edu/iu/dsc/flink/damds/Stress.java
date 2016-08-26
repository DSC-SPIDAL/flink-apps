package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.WeightsWrap1D;
import edu.iu.dsc.flink.mm.MatrixBlock;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import mpi.MPIException;

public class Stress {
  public static void stressFlink() {

  }

  private static double calculateStress(
      double[] preX, int targetDimension, double tCur, ShortMatrixBlock block,
      WeightsWrap1D weights, double invSumOfSquareDist, double[] internalPartialSigma)
      throws MPIException {
    double stress = 0.0;
    stress = calculateStressInternal(preX, targetDimension, tCur,
        block.getData(), weights);
    return stress;
  }

  private static double calculateStressInternal(double[] preX, int targetDim, double tCur,
                                                short[] distances, WeightsWrap1D weights) {

    double sigma = 0.0;
    double diff = 0.0;
    if (tCur > 10E-10) {
      diff = Math.sqrt(2.0 * targetDim) * tCur;
    }

    int threadRowCount = 0;
    final int globalRowOffset = 0;

    int globalColCount = ParallelOps.globalColCount;
    int globalRow, procLocalRow;
    double origD, weight, euclideanD;
    double heatD, tmpD;
    for (int localRow = 0; localRow < threadRowCount; ++localRow){
      globalRow = localRow + globalRowOffset;
      procLocalRow = globalRow - ParallelOps.procRowStartOffset;
      for (int globalCol = 0; globalCol < globalColCount; globalCol++) {
        origD = distances[procLocalRow * globalColCount + globalCol]
            * DAMDSUtils.INV_SHORT_MAX;
        weight = weights.getWeight(procLocalRow,globalCol);

        if (origD < 0 || weight == 0) {
          continue;
        }

        euclideanD = globalRow != globalCol ? DAMDSUtils.calculateEuclideanDist(
            preX, globalRow , globalCol, targetDim) : 0.0;

        heatD = origD - diff;
        tmpD = origD >= diff ? heatD - euclideanD : -euclideanD;
        sigma += weight * tmpD * tmpD;
      }
    }
    return sigma;
  }
}
