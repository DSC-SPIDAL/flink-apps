package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.MatrixUtils;
import edu.indiana.soic.spidal.common.RefObj;
import edu.indiana.soic.spidal.common.WeightsWrap1D;
import edu.iu.dsc.flink.mm.Matrix;
import mpi.MPIException;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class CG {
  private static void calculateConjugateGradient(DataSet<Matrix> preX, DataSet<Matrix> BC, DataSet<Matrix> vArray, Configuration parameters) {

  }

  private static DataSet<Matrix> calculateMM(DataSet<Matrix> A, DataSet<Matrix> B, Configuration parameters) {
    DataSet<Matrix> out = A.map(new RichMapFunction<Matrix, Tuple2<Integer, Matrix>>() {
      int targetDimension;
      int globalCols;

      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.targetDimension = parameters.getInteger(Constants.TARGET_DIMENSION, 3);
        this.globalCols = parameters.getInteger(Constants.GLOBAL_COLS, 0);
      }

      @Override
      public Tuple2<Integer, Matrix> map(Matrix matrx) throws Exception {
        List<Matrix> prex = getRuntimeContext().getBroadcastVariable("prex");
        Matrix preXM = prex.get(0);
        WeightsWrap1D weightsWrap1D = new WeightsWrap1D(null, null, false, globalCols);
        double []outMM = new double[globalCols * targetDimension];

        // todo figure out the details of the calculation
        calculateMMInternal(preXM.getData(), targetDimension, globalCols, weightsWrap1D, 64, matrx.getData(), outMM, matrx.getRows(), 0);
        Matrix out = new Matrix(outMM, matrx.getRows(), targetDimension, matrx.getIndex(), false);
        return new Tuple2<Integer, Matrix>(matrx.getIndex(), out);
      }
    }).withBroadcastSet(B, "prex").withParameters(parameters).reduceGroup(new RichGroupReduceFunction<Tuple2<Integer, Matrix>, Matrix>() {
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
    return out;
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
