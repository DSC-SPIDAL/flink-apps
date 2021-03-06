package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.WeightsWrap1D;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import mpi.MPIException;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

public class Stress {
  public static DataSet<Double> calculate(DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> distances, DataSet<Matrix> prexDataSet) {
    DataSet<Double> dataSet = distances.map(new RichMapFunction<Tuple2<ShortMatrixBlock, ShortMatrixBlock>, Tuple2<Integer, Double>>() {
      @Override
      public Tuple2<Integer, Double> map(Tuple2<ShortMatrixBlock, ShortMatrixBlock> tuple) throws Exception {
        List<Matrix> matrix = getRuntimeContext().getBroadcastVariable("prex");
        Matrix matrixB = matrix.get(0);

        ShortMatrixBlock distances = tuple.f0;
        ShortMatrixBlock weights = tuple.f1;
        double tCur = (double) matrixB.getProperties().get("tCur");
        double invs = (double) matrixB.getProperties().get("invs");
        WeightsWrap1D weightsWrap1D = new WeightsWrap1D(weights.getData(), null, false, weights.getMatrixCols());
        double stress = calculateStress(matrixB.getData(), matrixB.getCols(), tCur, distances, invs,
            distances.getBlockRows(), distances.getStart(), distances.getMatrixCols(), weightsWrap1D);
        return new Tuple2<Integer, Double>(0, stress);
      }
    }).withBroadcastSet(prexDataSet, "prex").reduceGroup(new GroupReduceFunction<Tuple2<Integer,Double>, Double>() {
      @Override
      public void reduce(Iterable<Tuple2<Integer, Double>> iterable, Collector<Double> collector) throws Exception {
        double sum = 0;
        for (Tuple2<Integer, Double> d : iterable) {
          sum += d.f1;
        }
        collector.collect(sum);
      }
    });
    return dataSet;
  }

  private static double calculateStress(
      double[] preX, int targetDimension, double tCur, ShortMatrixBlock block,
      double invSumOfSquareDist, int blockRowCount, int rowStartIndex, int globalColCount, WeightsWrap1D weights)
      throws MPIException {
    double stress;
    stress = calculateStressInternal(preX, targetDimension, tCur,
        block.getData(), blockRowCount, rowStartIndex, globalColCount, weights);
    return stress * invSumOfSquareDist;
  }

  private static double calculateStressInternal(double[] preX, int targetDim, double tCur,
                                                short[] distances, int blockRowCount,
                                                int rowStartIndex, int globalColCount, WeightsWrap1D weights) {

    double sigma = 0.0;
    double diff = 0.0;
    if (tCur > 10E-10) {
      diff = Math.sqrt(2.0 * targetDim) * tCur;
    }

    int globalRow, procLocalRow;
    double origD, weight, euclideanD;
    double heatD, tmpD;
    for (int localRow = 0; localRow < blockRowCount; ++localRow){
      globalRow = localRow + rowStartIndex;
      procLocalRow = localRow;
      for (int globalCol = 0; globalCol < globalColCount; globalCol++) {
        origD = distances[procLocalRow * globalColCount + globalCol]
            * DAMDSUtils.INV_SHORT_MAX;
        weight = weights.getWeight(procLocalRow, globalCol);
        if (origD < 0) {
          continue;
        }
        // System.out.printf("%d %d %d %d\n", globalCol, globalRow, targetDim, preX.length);
        try {
          euclideanD = globalRow != globalCol ? DAMDSUtils.calculateEuclideanDist(
              preX, globalRow, globalCol, targetDim) : 0.0;
          heatD = origD - diff;
          tmpD = origD >= diff ? heatD - euclideanD : -euclideanD;
          sigma += weight * tmpD * tmpD;
        } catch (ArrayIndexOutOfBoundsException e) {
          String format = String.format("%d %d %d %d %d %d", globalRow, globalCol, procLocalRow, targetDim, rowStartIndex, localRow);
          System.out.println(format);
          throw new RuntimeException(format, e);
        }
      }
    }
    return sigma;
  }
}
