package edu.iu.dsc.flink.damds;

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
  public static DataSet<Double> calculate(DataSet<ShortMatrixBlock> distances, DataSet<Matrix> prexDataSet) {
    DataSet<Double> dataSet = distances.map(new RichMapFunction<ShortMatrixBlock, Tuple2<Integer, Double>>() {
      @Override
      public Tuple2<Integer, Double> map(ShortMatrixBlock shortMatrixBlock) throws Exception {
        List<Matrix> matrix = getRuntimeContext().getBroadcastVariable("prex");
        Matrix matrixB = matrix.get(0);
        double tCur = (double) matrixB.getProperties().get("tCur");
        double invs = (double) matrixB.getProperties().get("invs");
        double stress = calculateStress(matrixB.getData(), matrixB.getCols(), tCur, shortMatrixBlock, invs,
            shortMatrixBlock.getBlockRows(), shortMatrixBlock.getStart(), shortMatrixBlock.getMatrixCols());
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
      double invSumOfSquareDist, int blockRowCount, int rowStartIndex, int globalColCount)
      throws MPIException {
    double stress;
    stress = calculateStressInternal(preX, targetDimension, tCur,
        block.getData(), blockRowCount, rowStartIndex, globalColCount);
    return stress * invSumOfSquareDist;
  }

  private static double calculateStressInternal(double[] preX, int targetDim, double tCur,
                                                short[] distances, int blockRowCount,
                                                int rowStartIndex, int globalColCount) {

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
      procLocalRow = globalRow - rowStartIndex;
      for (int globalCol = 0; globalCol < globalColCount; globalCol++) {
        origD = distances[procLocalRow * globalColCount + globalCol]
            * DAMDSUtils.INV_SHORT_MAX;
        weight = 1;
        if (origD < 0) {
          continue;
        }
        //System.out.printf("%d %d \n", globalCol, globalRow);
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
