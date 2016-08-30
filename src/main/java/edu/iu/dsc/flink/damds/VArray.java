package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.WeightsWrap1D;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;

import static edu.iu.dsc.flink.damds.DAMDSUtils.INV_SHORT_MAX;

public class VArray {
  public static DataSet<Matrix> generateVArray(DataSet<ShortMatrixBlock> distancesBlock, Configuration parameters) {
    DataSet<Matrix> dataSet = distancesBlock.map(new RichMapFunction<ShortMatrixBlock, Matrix>() {
      int targetDimention;
      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.targetDimention = parameters.getInteger(Constants.TARGET_DIMENSION, 3);
      }

      @Override
      public Matrix map(ShortMatrixBlock shortMatrixBlock) throws Exception {
        WeightsWrap1D weightsWrap1D = new WeightsWrap1D(null, null, false, 0);
        short[] distances = shortMatrixBlock.getData();
        double[] vArray = new double[shortMatrixBlock.getBlockRows() * targetDimention];
        generateVArrayInternal(distances, weightsWrap1D, vArray,
            shortMatrixBlock.getBlockRows(), shortMatrixBlock.getStart(),
            shortMatrixBlock.getMatrixCols());
        Matrix m = new Matrix(vArray, shortMatrixBlock.getBlockRows(), targetDimention,
            shortMatrixBlock.getIndex(), false);
        return m;
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
