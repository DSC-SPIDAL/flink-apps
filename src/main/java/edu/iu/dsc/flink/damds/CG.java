package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.MatrixUtils;
import edu.indiana.soic.spidal.common.WeightsWrap1D;
import edu.iu.dsc.flink.mm.Matrix;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class CG {
  public static DataSet<Matrix> calculateConjugateGradient(DataSet<Matrix> preX, DataSet<Matrix> BC,
                                                 DataSet<Matrix> vArray, Configuration parameters, int cgIter) {
    DataSet<Matrix> MMr = calculateMM(preX, vArray, parameters);
    DataSet<Matrix> newBC = MMr.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix MMR) throws Exception {
        List<Matrix> bcMatrix = getRuntimeContext().getBroadcastVariable("bc");
        Matrix BCM = bcMatrix.get(0);

        calculateMMRBC(MMR, BCM);
        return BCM;
      }
    }).withBroadcastSet(BC, "bc");

    DataSet<Matrix> newMMr = MMr.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix MMR) throws Exception {
        List<Matrix> bcMatrix = getRuntimeContext().getBroadcastVariable("bc");
        Matrix BCM = bcMatrix.get(0);

        calculateMMRBC(MMR, BCM);
        return MMR;
      }
    }).withBroadcastSet(BC, "bc");

    DataSet<Double> rTr = innerProductCalculation(newMMr);

    // now loop
    IterativeDataSet<Matrix> bcLoop = newBC.iterate(10);
    //IterativeDataSet<Matrix> bcLoop = newBC.iterate(cgIter);
    DataSet<Matrix> MMap = calculateMM(bcLoop, vArray, parameters);
    DataSet<Double> alpha = innerProductCalculation(bcLoop, MMap, rTr);
    // MMap.writeAsText("MMp.txt", FileSystem.WriteMode.OVERWRITE);

    DataSet<Matrix> newPrex = preX.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix matrix) throws Exception {
        System.out.println("CG Loop 1 *************************************************************************");
        List<Matrix> bcMatrixList = getRuntimeContext().getBroadcastVariable("bc");
        List<Double> alphaList = getRuntimeContext().getBroadcastVariable("alpha");
        Matrix bcMatrix = bcMatrixList.get(0);
        double alpha = alphaList.get(0);
        double []prex = matrix.getData();
        double []bc = bcMatrix.getData();
        //update Xi to Xi+1
        int iOffset;
        for(int i = 0; i < matrix.getRows(); ++i) {
          iOffset = i * matrix.getCols();
          for (int j = 0; j < matrix.getCols(); ++j) {
            prex[iOffset+j] += alpha * bc[iOffset+j];
          }
        }
        return matrix;
      }
    }).withBroadcastSet(bcLoop, "bc").withBroadcastSet(alpha, "alpha");

    // update MMr
    DataSet<Matrix> newMMr2 = MMap.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix matrix) throws Exception {
        System.out.println("CG Loop 2 **********************************************************************");
        List<Matrix> mmrMatrixList = getRuntimeContext().getBroadcastVariable("mmr");
        List<Double> alphaList = getRuntimeContext().getBroadcastVariable("alpha");
        double alpha = alphaList.get(0);
        double []mmap = matrix.getData();
        Matrix mmrMatrix = mmrMatrixList.get(0);
        double []mmr = mmrMatrix.getData();

        int iOffset;
        for(int i = 0; i < matrix.getRows(); ++i) {
          iOffset = i * matrix.getCols();
          for (int j = 0; j < matrix.getCols(); ++j) {
            mmr[iOffset+j] += alpha * mmap[iOffset+j];
          }
        }
        return mmrMatrix;
      }
    }).withBroadcastSet(newMMr, "mmr").withBroadcastSet(alpha, "alpha");

    DataSet<Double> rtr1 = innerProductCalculation(newMMr2);
    DataSet<Double> beta = devide(rtr1, rTr);

    DataSet<Matrix> newBC2 = bcLoop.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix matrix) throws Exception {
        System.out.println("CG Loop 3 **********************************************************************");
        List<Matrix> mmrMatrixList = getRuntimeContext().getBroadcastVariable("mmr");
        List<Double> betaList = getRuntimeContext().getBroadcastVariable("beta");
        double beta = betaList.get(0);
        double []bc = matrix.getData();
        Matrix mmrMatrix = mmrMatrixList.get(0);
        double []mmr = mmrMatrix.getData();

        int iOffset;
        for(int i = 0; i < matrix.getRows(); ++i) {
          iOffset = i * matrix.getCols();
          for (int j = 0; j < matrix.getCols(); ++j) {
            bc[iOffset+j] = mmr[iOffset+j] + beta * bc[iOffset+j];
          }
        }
        return matrix;
      }
    }).withBroadcastSet(newMMr, "mmr").withBroadcastSet(beta, "beta");
    // done with BC iterations
    DataSet<Matrix> finalBC = bcLoop.closeWith(newBC2);
    DataSet<Matrix> finalPrex = finalBC.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix matrix) throws Exception {
        List<Matrix> bcMatrixList = getRuntimeContext().getBroadcastVariable("prex");
        return bcMatrixList.get(0);
      }
    }).withBroadcastSet(newPrex, "prex");
    finalBC.writeAsText("bc2.txt", FileSystem.WriteMode.OVERWRITE);
    return finalPrex;
  }

  public static DataSet<Double> devide(DataSet<Double> a, DataSet<Double> b) {
    DataSet<Double> ab = a.map(new RichMapFunction<Double, Double>() {
      @Override
      public Double map(Double aDouble) throws Exception {
        List<Double> bList = getRuntimeContext().getBroadcastVariable("b");
        double b = bList.get(0);
        return aDouble / b;
      }
    }).withBroadcastSet(b, "b");
    return ab;
  }

  public static DataSet<Double> innerProductCalculation(DataSet<Matrix> aM, DataSet<Matrix> bM, DataSet<Double> rTr) {
    DataSet<Double> d = aM.map(new RichMapFunction<Matrix, Double>() {
      @Override
      public Double map(Matrix matrix) throws Exception {
        double []a = matrix.getData();
        List<Matrix> bMatrixList = getRuntimeContext().getBroadcastVariable("b");
        List<Double> rtrData = getRuntimeContext().getBroadcastVariable("rtr");
        double rtr = rtrData.get(0);
        Matrix bMatrix = bMatrixList.get(0);
        double []b = bMatrix.getData();
        double sum = 0;
        if (a.length > 0) {
          for (int i = 0; i < a.length; ++i) {
            sum += a[i] * b[i];
          }
        }
        return rtr / sum;
      }
    }).withBroadcastSet(bM, "b").withBroadcastSet(rTr, "rtr");
    return d;
  }

  private static DataSet<Double>  innerProductCalculation(DataSet<Matrix> m) {
    DataSet<Double> p = m.map(new MapFunction<Matrix, Double>() {
      @Override
      public Double map(Matrix matrix) throws Exception {
        double []a = matrix.getData();
        double sum = 0.0;
        if (a.length > 0) {
          for (double anA : a) {
            sum += anA * anA;
          }
        }
        return sum;
      }
    });
    return p;
  }

  private static void calculateMMRBC(Matrix MMR, Matrix BCM) {
    double []bcData = BCM.getData();
    double []mmrData = MMR.getData();

    int iOffset;
    for(int i = 0; i < MMR.getRows(); ++i) {
      iOffset = i * MMR.getCols();
      for (int j = 0; j < MMR.getCols(); ++j) {
        bcData[iOffset+j] -= mmrData[iOffset+j];
        mmrData[iOffset+j] = bcData[iOffset+j];
      }
    }
  }

  private static DataSet<Matrix> calculateMM(DataSet<Matrix> A, DataSet<Matrix> vArray, Configuration parameters) {
    DataSet<Matrix> out = vArray.map(new RichMapFunction<Matrix, Tuple2<Integer, Matrix>>() {
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
        System.out.println("Matrix multiply ***************************************");
        List<Matrix> prex = getRuntimeContext().getBroadcastVariable("prex");
        Matrix preXM = prex.get(0);
        WeightsWrap1D weightsWrap1D = new WeightsWrap1D(null, null, false, globalCols);
        double []outMM = new double[matrx.getRows() * targetDimension];

        // todo figure out the details of the calculation
        calculateMMInternal(preXM.getData(), targetDimension, globalCols, weightsWrap1D, 64, matrx.getData(), outMM, matrx.getRows(), 0);
        Matrix out = new Matrix(outMM, matrx.getRows(), targetDimension, matrx.getIndex(), false);
        return new Tuple2<Integer, Matrix>(matrx.getIndex(), out);
      }
    }).withBroadcastSet(A, "prex").withParameters(parameters).reduceGroup(new RichGroupReduceFunction<Tuple2<Integer, Matrix>, Matrix>() {
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
