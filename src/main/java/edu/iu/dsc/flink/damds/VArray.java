package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.WeightsWrap1D;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import static edu.iu.dsc.flink.damds.DAMDSUtils.INV_SHORT_MAX;

public class VArray {
  public static DataSet<Tuple2<Matrix, ShortMatrixBlock>> generateVArray(
      DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> distancesWeights,
      Configuration parameters) {
    DataSet<Tuple2<Matrix, ShortMatrixBlock>> dataSet = distancesWeights.map(
        new RichMapFunction<Tuple2<ShortMatrixBlock, ShortMatrixBlock>, Tuple2<Matrix, ShortMatrixBlock>>() {
      int targetDimention;
      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.targetDimention = parameters.getInteger(Constants.TARGET_DIMENSION, 3);
      }

      @Override
      public Tuple2<Matrix, ShortMatrixBlock> map(Tuple2<ShortMatrixBlock, ShortMatrixBlock> shortMatrixBlock) throws Exception {
        ShortMatrixBlock weights = shortMatrixBlock.f1;
        ShortMatrixBlock distanceMatrixBlock = shortMatrixBlock.f0;

        WeightsWrap1D weightsWrap1D = new WeightsWrap1D(weights.getData(), null, false, weights.getMatrixCols());
        short[] distances = distanceMatrixBlock.getData();
        double[] vArray = new double[distanceMatrixBlock.getBlockRows()];
        generateVArrayInternal(distances, weightsWrap1D, vArray,
            distanceMatrixBlock.getBlockRows(), distanceMatrixBlock.getStart(),
            distanceMatrixBlock.getMatrixCols());
        Matrix m = new Matrix(vArray, distanceMatrixBlock.getBlockRows(), 1,
            distanceMatrixBlock.getIndex(), false);
        m.setStartIndex(distanceMatrixBlock.getStart());
        return new Tuple2<Matrix, ShortMatrixBlock>(m, weights);
      }
    }).withParameters(parameters);

    return dataSet;
  }

  private static void generateVArrayInternal(
      short[] distances, WeightsWrap1D weights, double[] v, int blockRowCount,
      int rowStartIndex, int globalColCount) {
    for (int i = 0; i < blockRowCount; ++i) {
      int globalRow = i + rowStartIndex;
      for (int globalCol = 0; globalCol < globalColCount; ++globalCol) {
        if (globalRow == globalCol) continue;

        double origD = distances[i * globalColCount + globalCol] * INV_SHORT_MAX;
        double weight = weights.getWeight(i, globalCol);

        if (origD < 0 || weight == 0) {
          continue;
        }
        v[i] += weight;
      }
      v[i] += 1;
    }
  }
}
