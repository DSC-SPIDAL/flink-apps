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
  public static DataSet<Tuple2<Matrix, ShortMatrixBlock>> generateVArray(DataSet<ShortMatrixBlock> distancesBlock,
                                                                         DataSet<ShortMatrixBlock> weightBlock,
                                                                         Configuration parameters) {
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
        double[] vArray = new double[shortMatrixBlock.getBlockRows()];
        generateVArrayInternal(distances, weightsWrap1D, vArray,
            shortMatrixBlock.getBlockRows(), shortMatrixBlock.getStart(),
            shortMatrixBlock.getMatrixCols());
        Matrix m = new Matrix(vArray, shortMatrixBlock.getBlockRows(), 1,
            shortMatrixBlock.getIndex(), false);
        m.setStartIndex(shortMatrixBlock.getStart());
        return m;
      }
    }).withParameters(parameters);

    DataSet<Tuple2<Matrix, ShortMatrixBlock>> joinset = dataSet.join(weightBlock).where(new KeySelector<Matrix, Integer>() {
      @Override
      public Integer getKey(Matrix matrix) throws Exception {
        return matrix.getIndex();
      }
    }).equalTo(new KeySelector<ShortMatrixBlock, Integer>() {
      @Override
      public Integer getKey(ShortMatrixBlock shortMatrixBlock) throws Exception {
        return shortMatrixBlock.getIndex();
      }
    }).with(new JoinFunction<Matrix, ShortMatrixBlock, Tuple2<Matrix, ShortMatrixBlock>>() {
      @Override
      public Tuple2<Matrix, ShortMatrixBlock> join(Matrix matrix, ShortMatrixBlock shortMatrixBlock) throws Exception {
        return new Tuple2<Matrix, ShortMatrixBlock>(matrix, shortMatrixBlock);
      }
    });

    return joinset;
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
