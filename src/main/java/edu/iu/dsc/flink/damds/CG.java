package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.MatrixUtils;
import edu.indiana.soic.spidal.common.RefObj;
import edu.indiana.soic.spidal.common.WeightsWrap1D;
import edu.iu.dsc.flink.mm.Matrix;
import mpi.MPIException;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;

import java.util.List;

public class CG {
  private static void calculateConjugateGradient(DataSet<Integer> parallel, DataSet<Matrix> preX, DataSet<Matrix> BC) {
    parallel.map(new RichMapFunction<Integer, Matrix>() {
      @Override
      public Matrix map(Integer integer) throws Exception {
        List<Matrix> prex = getRuntimeContext().getBroadcastVariable("prex");
        List<Matrix> BC = getRuntimeContext().getBroadcastVariable("bc");

        return null;
      }
    }).withBroadcastSet(preX, "prex").withBroadcastSet(BC, "bc");
  }

  private static DataSet<Matrix> calculateMM(Matrix A, Matrix B) {
    return null;
  }

  private static void calculateConjugateGradient(
      double[] preX, int targetDimension, int numPoints, double[] BC, int cgIter, double cgThreshold,
      RefObj<Integer> outCgCount, RefObj<Integer> outRealCGIterations,
      int blockSize, double[] vArray, double[] MMr, double[] MMAp, double[][] threadPartialMM)

      throws MPIException {
    WeightsWrap1D weights = new WeightsWrap1D(null, null, false, 1);
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
      int blockSize, double[] vArray, double[] outMM,
      double[][] internalPartialMM) throws MPIException {
     
  }

  private static void calculateMMInternal(
      double[] x, int targetDimension, int numPoints,
      WeightsWrap1D weights, int blockSize, double[] vArray, double[] outMM, int rowCount, int rowStartOffset) {

    MatrixUtils
        .matrixMultiplyWithThreadOffset(weights, vArray, x,
            rowCount, targetDimension,
            numPoints, blockSize,
            rowStartOffset,
            rowStartOffset, outMM);
  }
}
