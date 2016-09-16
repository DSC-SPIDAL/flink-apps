package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.DoubleStatistics;
import edu.iu.dsc.flink.mm.Matrix;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

import static edu.iu.dsc.flink.damds.DAMDSUtils.INV_SHORT_MAX;
import static edu.iu.dsc.flink.damds.DAMDSUtils.SHORT_MAX;

public class Distances {
  public static DataSet<ShortMatrixBlock> updateDistances(DataSet<ShortMatrixBlock> distances,
                                                          DataSet<DoubleStatistics> stats) {
    DataSet<ShortMatrixBlock> updates = distances.map(new RichMapFunction<ShortMatrixBlock, ShortMatrixBlock>() {
      @Override
      public ShortMatrixBlock map(ShortMatrixBlock shortMatrixBlock) throws Exception {
        List<DoubleStatistics> statsList = getRuntimeContext().getBroadcastVariable("stats");
        DoubleStatistics stats = statsList.get(0);
        changeZeroDistancesToPostiveMin(shortMatrixBlock.getData(), stats.getPositiveMin());
        //System.out.println("Update distances");
        return shortMatrixBlock;
      }
    }).withBroadcastSet(stats, "stats");
    return updates;
  }

  public static DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> calculate(DataSet<ShortMatrixBlock> distances,
                                                                              DataSet<ShortMatrixBlock> weights) {
    DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> distanceWeights =
        distances.join(weights).where(new KeySelector<ShortMatrixBlock, Integer>() {
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
      public Tuple2<ShortMatrixBlock, ShortMatrixBlock> join(ShortMatrixBlock distances,
                                                             ShortMatrixBlock weights) throws Exception {
        //System.out.println("Join distances");
        return new Tuple2<ShortMatrixBlock, ShortMatrixBlock>(distances, weights);
      }
    });

    return distanceWeights;
  }

  private static void changeZeroDistancesToPostiveMin(
      short[] distances, double positiveMin) {
    double tmpD;
    for (int i = 0; i < distances.length; ++i){
      tmpD = distances[i] * INV_SHORT_MAX;
      if (tmpD < positiveMin && tmpD >= 0.0){
        distances[i] = (short)(positiveMin * SHORT_MAX);
      }
    }
  }
}
