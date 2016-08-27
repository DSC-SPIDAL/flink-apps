package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.MatrixUtils;
import edu.indiana.soic.spidal.common.RefObj;
import edu.indiana.soic.spidal.common.WeightsWrap1D;
import mpi.MPIException;


public class CG {
  private static void calculateConjugateGradient(
      double[] preX, int targetDimension, int numPoints, double[] BC, int cgIter, double cgThreshold,
      RefObj<Integer> outCgCount, RefObj<Integer> outRealCGIterations,
      WeightsWrap1D weights, int blockSize, double[][] vArray, double[] MMr, double[] MMAp, double[][] threadPartialMM)

      throws MPIException {

    DAMDSUtils.zeroOutArray(threadPartialMM);
    calculateMM(preX, targetDimension, numPoints, weights, blockSize,
        vArray, MMr, threadPartialMM);

    // This barrier was necessary for correctness when using
    // a single mmap file

    int iOffset;
    double[] tmpRHSRow;

    for(int i = 0; i < numPoints; ++i) {
      iOffset = i*targetDimension;
      for (int j = 0; j < targetDimension; ++j) {
        BC[iOffset+j] -= MMr[iOffset+j];
        MMr[iOffset+j] = BC[iOffset+j];
      }
    }

    int cgCount = 0;
    double rTr = DAMDSUtils.innerProductCalculation(MMr);
    // Adding relative value test for termination as suggested by Dr. Fox.
    double testEnd = rTr * cgThreshold;

    while(cgCount < cgIter){
      cgCount++;
      outRealCGIterations.setValue(outRealCGIterations.getValue() + 1);

      //calculate alpha
      DAMDSUtils.zeroOutArray(threadPartialMM);
      calculateMM(BC, targetDimension, numPoints, weights, blockSize,
          vArray, MMAp, threadPartialMM);

      double alpha = rTr
          / DAMDSUtils.innerProductCalculation(BC, MMAp);

      //update Xi to Xi+1
      for(int i = 0; i < numPoints; ++i) {
        iOffset = i*targetDimension;
        for (int j = 0; j < targetDimension; ++j) {
          preX[iOffset+j] += alpha * BC[iOffset+j];
        }
      }

      if (rTr < testEnd) {
        break;
      }

      //update ri to ri+1
      for(int i = 0; i < numPoints; ++i) {
        iOffset = i*targetDimension;
        for (int j = 0; j < targetDimension; ++j) {
          MMr[iOffset+j] -= alpha * MMAp[iOffset+j];
        }
      }

      //calculate beta
      double rTr1 = DAMDSUtils.innerProductCalculation(MMr);
      double beta = rTr1/rTr;
      rTr = rTr1;

      //update pi to pi+1
      for(int i = 0; i < numPoints; ++i) {
        iOffset = i*targetDimension;
        for (int j = 0; j < targetDimension; ++j) {
          BC[iOffset+j] = MMr[iOffset+j] + beta * BC[iOffset+j];
        }
      }

    }
    outCgCount.setValue(outCgCount.getValue() + cgCount);
  }

  private static void calculateMM(
      double[] x, int targetDimension, int numPoints, WeightsWrap1D weights,
      int blockSize, double[][] vArray, double[] outMM,
      double[][] internalPartialMM) throws MPIException {

//    if (ParallelOps.threadCount > 1) {
//      launchHabaneroApp(
//          () -> forallChunked(
//              0, ParallelOps.threadCount - 1,
//              (threadIdx) -> {
//                calculateMMInternal(threadIdx, x, targetDimension,
//                    numPoints, weights, blockSize,
//                    vArray,
//                    internalPartialMM[threadIdx]);
//              }));
//    }
//    else {
//      calculateMMInternal(0, x, targetDimension, numPoints, weights,
//          blockSize, vArray, internalPartialMM[0]);
//    }

//    if (ParallelOps.worldProcsCount > 1) {
//      // // TODO: 8/24/16
//      // DAMDSUtils.mergePartials(internalPartialMM, ParallelOps.mmapXWriteBytes);
//
//      // Important barrier here - as we need to make sure writes are done to the mmap file
//      // it's sufficient to wait on ParallelOps.mmapProcComm, but it's cleaner for timings
//      // if we wait on the whole world
//
//      // Each process in a memory group waits here.
//      // It's not necessary to wait for a process
//      // in another memory map group, hence the use of mmapProcComm.
//      // However it's cleaner for any timings to have everyone sync here,
//      // so will use worldProcsComm instead.
//    } else {
//      DAMDSUtils.mergePartials(internalPartialMM, outMM);
//    }
  }

  private static void calculateMMInternal(
      Integer threadIdx, double[] x, int targetDimension, int numPoints,
      WeightsWrap1D weights, int blockSize, double[][] vArray, double[] outMM) {

//    MatrixUtils
//        .matrixMultiplyWithThreadOffset(weights, vArray[threadIdx], x,
//            ParallelOps.threadRowCounts[threadIdx], targetDimension,
//            numPoints, blockSize,
//            ParallelOps.threadRowStartOffsets[threadIdx],
//            ParallelOps.threadRowStartOffsets[threadIdx]
//                + ParallelOps.procRowStartOffset, outMM);
  }
}
