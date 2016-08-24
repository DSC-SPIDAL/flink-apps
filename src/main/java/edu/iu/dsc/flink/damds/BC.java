package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.MatrixUtils;
import edu.indiana.soic.spidal.common.WeightsWrap1D;
import mpi.MPIException;

public class BC {
  public static final double INV_SHORT_MAX = 1.0 / Short.MAX_VALUE;

  public static void calculateBC(
      double[] preX, int targetDimension, double tCur, short[] distances,
      WeightsWrap1D weights, int blockSize, double[] BC,
      double[][][] threadPartialBCInternalBofZ,
      double[][] threadPartialBCInternalMM)
      throws MPIException, InterruptedException {

    calculateBCInternal(
        0, preX, targetDimension, tCur, distances, weights, blockSize,
        threadPartialBCInternalBofZ[0], threadPartialBCInternalMM[0]);

    if (ParallelOps.worldProcsCount > 1) {
      // // TODO: 8/24/16
      //DAMDSUtils.mergePartials(threadPartialBCInternalMM, ParallelOps.mmapXWriteBytes);
      // Important barrier here - as we need to make sure writes are done to the mmap file
      // it's sufficient to wait on ParallelOps.mmapProcComm, but it's cleaner for timings
      // if we wait on the whole world
      ParallelOps.worldProcsComm.barrier();

      if (ParallelOps.isMmapLead) {
        ParallelOps.partialXAllGather();
      }
      // Each process in a memory group waits here.
      // It's not necessary to wait for a process
      // in another memory map group, hence the use of mmapProcComm.
      // However it's cleaner for any timings to have everyone sync here,
      // so will use worldProcsComm instead.
      ParallelOps.worldProcsComm.barrier();

      DAMDSUtils.extractPoints(ParallelOps.fullXBytes,
          ParallelOps.globalColCount,
          targetDimension, BC);
    } else {
      DAMDSUtils.mergePartials(threadPartialBCInternalMM, BC);
    }
  }

  private static void calculateBCInternal(
      Integer threadIdx, double[] preX, int targetDimension, double tCur,
      short[] distances, WeightsWrap1D weights, int blockSize,
      double[][] internalBofZ, double[] outMM) {

    calculateBofZ(threadIdx, preX, targetDimension, tCur,
        distances, weights, internalBofZ);

    // Next we can calculate the BofZ * preX.
    MatrixUtils.matrixMultiply(internalBofZ, preX,
        ParallelOps.threadRowCounts[threadIdx], targetDimension,
        ParallelOps.globalColCount, blockSize, outMM);
  }

  private static void calculateBofZ(
      int threadIdx, double[] preX, int targetDimension, double tCur, short[] distances, WeightsWrap1D weights,
      double[][] outBofZ) {

    int threadRowCount = ParallelOps.threadRowCounts[threadIdx];

    double vBlockValue = -1;

    double diff = 0.0;
    if (tCur > 10E-10) {
      diff = Math.sqrt(2.0 * targetDimension) * tCur;
    }

    short[] distancesProcLocalRow;
    double[] outBofZLocalRow;
    double[] preXGlobalRow;
    double origD, weight, dist;

    final int globalColCount = ParallelOps.globalColCount;
    final int globalRowOffset = ParallelOps.threadRowStartOffsets[threadIdx]
        + ParallelOps.procRowStartOffset;
    int globalRow, procLocalRow;
    for (int localRow = 0; localRow < threadRowCount; ++localRow) {
      globalRow = localRow + globalRowOffset;
      procLocalRow = globalRow - ParallelOps.procRowStartOffset;
      outBofZLocalRow = outBofZ[localRow];
      outBofZLocalRow[globalRow] = 0;
      for (int globalCol = 0; globalCol < ParallelOps.globalColCount; globalCol++) {
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


        origD = distances[procLocalRow * globalColCount + globalCol] * INV_SHORT_MAX;
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
