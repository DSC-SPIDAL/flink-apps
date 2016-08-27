package edu.iu.dsc.flink.damds;

import edu.indiana.soic.spidal.common.DoubleStatistics;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Collector;

public class Statistics {
  public static DataSet<DoubleStatistics> calculateStatistics(DataSet<ShortMatrixBlock> matrixBlockDataSet) {
    DataSet<DoubleStatistics> stats = matrixBlockDataSet.flatMap(new RichFlatMapFunction<ShortMatrixBlock, DoubleStatistics>() {
      @Override
      public void flatMap(ShortMatrixBlock shortMatrixBlock, Collector<DoubleStatistics> collector) throws Exception {
        DoubleStatistics doubleStatistics = calculateStatisticsInternal(shortMatrixBlock.getData(), shortMatrixBlock.getBlockRows(),
            shortMatrixBlock.getStart(), shortMatrixBlock.getMatrixCols());
        collector.collect(doubleStatistics);
      }
    }).reduce(new ReduceFunction<DoubleStatistics>() {
      @Override
      public DoubleStatistics reduce(DoubleStatistics doubleStatistics, DoubleStatistics t1) throws Exception {
        doubleStatistics.combine(t1);
        return doubleStatistics;
      }
    });
    return stats;
  }

  private static DoubleStatistics calculateStatisticsInternal(
      short[] distances, int blockRowCount, int rowStartIndex, int globalColCount) {
    DoubleStatistics stat = new DoubleStatistics();
    int procLocalRow;
    double origD, weight;
    for (int localRow = 0; localRow < blockRowCount; ++localRow){
      procLocalRow = localRow + rowStartIndex;
      for (int globalCol = 0; globalCol < globalColCount; globalCol++) {
        origD = distances[procLocalRow * globalColCount + globalCol] * DAMDSUtils.INV_SHORT_MAX;
        if (origD < 0) {
          // Missing distance
          continue;
        }
        stat.accept(origD);
      }
    }
    return stat;
  }
}
