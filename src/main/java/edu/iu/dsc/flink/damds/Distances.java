package edu.iu.dsc.flink.damds;

import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class Distances {
  public static DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> calculate(DataSet<ShortMatrixBlock> distances, DataSet<ShortMatrixBlock> weights) {
    DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> distanceWeights = distances.join(weights).where(new KeySelector<ShortMatrixBlock, Integer>() {
      @Override
      public Integer getKey(ShortMatrixBlock matrix) throws Exception {
        return matrix.getIndex();
      }
    }).equalTo(new KeySelector<ShortMatrixBlock, Integer>() {
      @Override
      public Integer getKey(ShortMatrixBlock shortMatrixBlock) throws Exception {
        return shortMatrixBlock.getIndex();
      }
    }).with(new JoinFunction<ShortMatrixBlock, ShortMatrixBlock, Tuple2<ShortMatrixBlock, ShortMatrixBlock>>() {
      @Override
      public Tuple2<ShortMatrixBlock, ShortMatrixBlock> join(ShortMatrixBlock matrix, ShortMatrixBlock shortMatrixBlock) throws Exception {
        return new Tuple2<ShortMatrixBlock, ShortMatrixBlock>(matrix, shortMatrixBlock);
      }
    });

    return distanceWeights;
  }
}
