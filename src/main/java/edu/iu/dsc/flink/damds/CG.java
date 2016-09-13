package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.MatrixUtils;
import edu.indiana.soic.spidal.common.WeightsWrap1D;
import edu.iu.dsc.flink.mm.Matrix;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class CG {
  public static DataSet<Matrix> calculateConjugateGradient(DataSet<Matrix> preX, DataSet<Matrix> BC,
                                                 DataSet<Matrix> vArray, Configuration parameters, int cgIter) {
    DataSet<Matrix> MMr = calculateMM(preX, vArray, parameters);
    MMr.writeAsText("mmr0", FileSystem.WriteMode.OVERWRITE);
    DataSet<Matrix> newBC = MMr.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix MMR) throws Exception {
        List<Matrix> bcMatrix = getRuntimeContext().getBroadcastVariable("bc");
        Matrix BCM = bcMatrix.get(0);

        calculateMMRBC(MMR, BCM);
        return BCM;
      }
    }).withBroadcastSet(BC, "bc");

    DataSet<Matrix> MMr_1 = MMr.map(new RichMapFunction<Matrix, Matrix>() {
      @Override
      public Matrix map(Matrix MMR) throws Exception {
        List<Matrix> bcMatrix = getRuntimeContext().getBroadcastVariable("bc");
        Matrix BCM = bcMatrix.get(0);

        calculateMMRBC(MMR, BCM);
        double rTr = InnerProductMatrix(MMR);
        System.out.println("###########################  init rTr: " + rTr);
        MMR.addProperty("rTr", rTr);
        MMR.addProperty("testEnd", rTr * 0.00001);
        MMR.addProperty("break", false);
        return MMR;
      }
    }).withBroadcastSet(BC, "bc");

    MMr_1.writeAsText("mmr1", FileSystem.WriteMode.OVERWRITE);
    newBC.writeAsText("bc2", FileSystem.WriteMode.OVERWRITE);

    // now compbine prex and bc because flink cannot loop over bc and return prex
    DataSet<Tuple3<Matrix, Matrix, Matrix>> prexbc = newBC.map(new RichMapFunction<Matrix, Tuple3<Matrix, Matrix, Matrix>>() {
      @Override
      public Tuple3<Matrix, Matrix, Matrix> map(Matrix bcMatrix) throws Exception {
        List<Matrix> prexMatrixList = getRuntimeContext().getBroadcastVariable("prex");
        List<Matrix> mmrMatrixList = getRuntimeContext().getBroadcastVariable("mmr");
        Matrix prexMatrix = prexMatrixList.get(0);
        Matrix mmrMatrix = mmrMatrixList.get(0);
        return new Tuple3<Matrix, Matrix, Matrix>(prexMatrix, bcMatrix, mmrMatrix);
      }
    }).withBroadcastSet(preX, "prex").withBroadcastSet(MMr_1, "mmr");

    // now loop
    IterativeDataSet<Tuple3<Matrix, Matrix, Matrix>> prexbcloop = prexbc.iterate(cgIter);
    //IterativeDataSet<Matrix> bcLoop = newBC.iterate(cgIter);
    DataSet<Matrix> MMap = calculateMMBC(prexbcloop, vArray, parameters);

    DataSet<Tuple3<Matrix, Matrix, Matrix>> newLoop = prexbcloop.map(new RichMapFunction<Tuple3<Matrix, Matrix, Matrix>, Tuple3<Matrix, Matrix, Matrix>>() {
      @Override
      public Tuple3<Matrix, Matrix, Matrix> map(Tuple3<Matrix, Matrix, Matrix> loop) throws Exception {
        List<Matrix> mmapList = getRuntimeContext().getBroadcastVariable("mmap");
        Matrix bcMatrix = loop.f1;
        Matrix prexMatrix = loop.f0;
        Matrix mmrMatrix = loop.f2;
        Matrix mmapMatrix = mmapList.get(0);
        writeToFile("mmap", mmapMatrix.toString());

        double []prex = prexMatrix.getData();
        double []bc = bcMatrix.getData();
        double []mmr = mmrMatrix.getData();
        double []mmap = mmapMatrix.getData();

        double rtr = (double) mmrMatrix.getProperties().get("rTr");
        double innerProduct = innerProductCalculation(bc, mmap);
        System.out.println("********************* Inner product: " + innerProduct);
        double alpha = rtr / innerProduct;
        System.out.println("********************* Alpha: " + alpha);
        //update Xi to Xi+1
        int iOffset;
        int numPoints = prexMatrix.getRows();
        int targetDimension = prexMatrix.getCols();
        for(int i = 0; i < numPoints; ++i) {
          iOffset = i * targetDimension;
          for (int j = 0; j < targetDimension; ++j) {
            prex[iOffset+j] += alpha * bc[iOffset+j];
          }
        }

        double testEnd = (double) mmrMatrix.getProperties().get("testEnd");
        if (rtr < testEnd) {
          mmrMatrix.addProperty("break", true);
        }

        //update ri to ri+1
        for(int i = 0; i < numPoints; ++i) {
          iOffset = i * targetDimension;
          for (int j = 0; j < targetDimension; ++j) {
            mmr[iOffset+j] -= alpha * mmap[iOffset+j];
          }
        }

        double rtr1 = InnerProductMatrix(mmrMatrix);
        System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&& rtr1: " + rtr1);
        double beta = rtr1 / rtr;
        System.out.println("############################## beta: " + beta);
        mmrMatrix.addProperty("rTr", rtr1);
        //update pi to pi+1
        for(int i = 0; i < numPoints; ++i) {
          iOffset = i * targetDimension;
          for (int j = 0; j < targetDimension; ++j) {
            bc[iOffset+j] = mmr[iOffset+j] + beta * bc[iOffset+j];
          }
        }

        writeToFile("point", prexMatrix.toString());
        return loop;
      }
    }).withBroadcastSet(MMap, "mmap");

    // done with BC iterations
    DataSet<Tuple3<Matrix, Matrix, Matrix>> finalBC = prexbcloop.closeWith(newLoop, newLoop.filter(new FilterFunction<Tuple3<Matrix, Matrix, Matrix>>() {
      @Override
      public boolean filter(Tuple3<Matrix, Matrix, Matrix> loop) throws Exception {
        Matrix mmrMatrix = loop.f2;
        return (boolean) mmrMatrix.getProperties().get("break");
      }
    }));
    DataSet<Matrix> prex = finalBC.map(new RichMapFunction<Tuple3<Matrix, Matrix, Matrix>, Matrix>() {
      @Override
      public Matrix map(Tuple3<Matrix, Matrix, Matrix> loop) throws Exception {
        return loop.f0;
      }
    });

    return prex;
  }

  private static void writeToFile(String file, String content) {
    try {
      PrintWriter out = new PrintWriter(file);
      out.write(content);
      out.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }

  private static double innerProductCalculation(double[] a, double[] b) {
    double sum = 0;
    if (a.length > 0) {
      for (int i = 0; i < a.length; ++i) {
        sum += a[i] * b[i];
      }
    }
    return sum;
  }

  public static DataSet<Double> devide(DataSet<Double> a, DataSet<Double> b) {
    DataSet<Double> ab = a.map(new RichMapFunction<Double, Double>() {
      @Override
      public Double map(Double aDouble) throws Exception {
        List<Double> bList = getRuntimeContext().getBroadcastVariable("b");
        double b = bList.get(0);
        double v = aDouble / b;
        System.out.println("################################ Beta: " + v);
        return v;
      }
    }).withBroadcastSet(b, "b");
    return ab;
  }

  public static DataSet<Double> bcInnerProductCalculation(DataSet<Tuple3<Matrix, Matrix, Matrix>> aM, DataSet<Matrix> bM) {
    DataSet<Double> d = aM.map(new RichMapFunction<Tuple3<Matrix, Matrix, Matrix>, Double>() {
      @Override
      public Double map(Tuple3<Matrix, Matrix, Matrix> matrix) throws Exception {
        double []a = matrix.f1.getData();
        List<Matrix> bMatrixList = getRuntimeContext().getBroadcastVariable("b");
        double rtr = (double) matrix.f2.getProperties().get("rTr");
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
    }).withBroadcastSet(bM, "b");
    return d;
  }

  private static DataSet<Double> bcInnerProductCalculation(DataSet<Matrix> m) {
    DataSet<Double> p = m.map(new MapFunction<Matrix, Double>() {
      @Override
      public Double map(Matrix matrix) throws Exception {
        return InnerProductMatrix(matrix);
      }
    });
    return p;
  }

  private static Double InnerProductMatrix(Matrix matrix) {
    double []a = matrix.getData();
    double sum = 0.0;
    if (a.length > 0) {
      for (double anA : a) {
        sum += anA * anA;
      }
    }

    return sum;
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
        calculateMMInternal(preXM.getData(), targetDimension, globalCols, weightsWrap1D, 32, matrx.getData(), outMM, matrx.getRows(), matrx.getStartIndex());
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

  private static DataSet<Matrix> calculateMMBC(DataSet<Tuple3<Matrix, Matrix, Matrix>> A, DataSet<Matrix> vArray, Configuration parameters) {
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
        List<Tuple3<Matrix, Matrix, Matrix>> prex = getRuntimeContext().getBroadcastVariable("cgloop");
        Matrix preXM = prex.get(0).f1;
        WeightsWrap1D weightsWrap1D = new WeightsWrap1D(null, null, false, globalCols);
        double []outMM = new double[matrx.getRows() * targetDimension];

        // todo figure out the details of the calculation
        calculateMMInternal(preXM.getData(), targetDimension, globalCols, weightsWrap1D, 32, matrx.getData(), outMM, matrx.getRows(), matrx.getStartIndex());
        Matrix out = new Matrix(outMM, matrx.getRows(), targetDimension, matrx.getIndex(), false);
        return new Tuple2<Integer, Matrix>(matrx.getIndex(), out);
      }
    }).withBroadcastSet(A, "cgloop").withParameters(parameters).reduceGroup(new RichGroupReduceFunction<Tuple2<Integer, Matrix>, Matrix>() {
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
