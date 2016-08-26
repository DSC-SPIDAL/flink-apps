package edu.iu.dsc.flink.damds;

import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import mpi.MPIException;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.List;

public class Stress {
  public static DataSet<Double> setupWorkFlow(DataSet<ShortMatrixBlock> distances, DataSet<Matrix> prexDataSet) {
    String[] args = new String[2];
    ParameterTool parameters = ParameterTool.fromArgs(args);

    DataSet<Double> dataSet = distances.map(new RichMapFunction<ShortMatrixBlock, Tuple2<Integer, Double>>() {
      @Override
      public Tuple2<Integer, Double> map(ShortMatrixBlock shortMatrixBlock) throws Exception {
        List<Matrix> matrix = getRuntimeContext().getBroadcastVariable("prex");
        Matrix matrixB = matrix.get(0);
        // todo
        double stress = calculateStress(matrixB.getData(), matrixB.getCols(), 0, shortMatrixBlock, 0, new double[4]);
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
      double invSumOfSquareDist, double[] internalPartialSigma)
      throws MPIException {
    double stress = 0.0;
    stress = calculateStressInternal(preX, targetDimension, tCur,
        block.getData());
    return stress;
  }

  private static double calculateStressInternal(double[] preX, int targetDim, double tCur, short[] distances) {

    double sigma = 0.0;
    double diff = 0.0;
    if (tCur > 10E-10) {
      diff = Math.sqrt(2.0 * targetDim) * tCur;
    }

    // todo
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
        weight = 1;

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
