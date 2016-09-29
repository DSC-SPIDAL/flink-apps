package edu.iu.dsc.flink.damds;

import com.google.common.io.LittleEndianDataInputStream;
import edu.indiana.soic.spidal.common.DoubleStatistics;
import edu.iu.dsc.flink.mm.ShortMatrixBlock;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

  public static DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> join(DataSet<ShortMatrixBlock> distances,
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

  public static DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> filReadJoin(DataSet<ShortMatrixBlock> distances,
                                                                         Configuration parameters) {
    DataSet<Tuple2<ShortMatrixBlock, ShortMatrixBlock>> distanceWeights =
        distances.map(new RichMapFunction<ShortMatrixBlock, Tuple2<ShortMatrixBlock, ShortMatrixBlock>>() {
          String weightFile;
          boolean isBigEndian;
          @Override
          public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.weightFile = parameters.getString(Constants.WEIGHT_FILE, "weight.bin");
            this.isBigEndian = parameters.getBoolean(Constants.BIG_INDIAN, true);
          }

          @Override
          public Tuple2<ShortMatrixBlock, ShortMatrixBlock> map(ShortMatrixBlock distanceBlock) throws Exception {
            ShortMatrixBlock weightBlock = new ShortMatrixBlock();
            weightBlock.setBlockRows(distanceBlock.getBlockRows());
            weightBlock.setMatrixCols(distanceBlock.getMatrixCols());
            weightBlock.setMatrixRows(distanceBlock.getMatrixRows());
            weightBlock.setIndex(distanceBlock.getIndex());
            weightBlock.setStart(distanceBlock.getStart());

            // check weather this is HDFS
            String protocolSplit[] = weightFile.split(":");
            if (protocolSplit.length > 1 && protocolSplit[0].trim().equals("hdfs")) {
              readHDFSFile(weightFile, isBigEndian, weightBlock);
            } else {
              // regular file
              readFile(weightFile, isBigEndian, weightBlock);
            }
            return new Tuple2<ShortMatrixBlock, ShortMatrixBlock>(distanceBlock, weightBlock);
          }
        }).withParameters(parameters);

    return distanceWeights;
  }

  private static void readHDFSFile(String weightFile, boolean isBigEndian, ShortMatrixBlock weightBlock) throws Exception {
    Path pt = new Path(weightFile);
    FileSystem fs = FileSystem.get(new org.apache.hadoop.conf.Configuration());
    // now read the data from the file
    try (BufferedInputStream pointBufferedStream = new BufferedInputStream(fs.open(pt))) {
      DataInput pointStream = isBigEndian ? new DataInputStream(
          pointBufferedStream) : new LittleEndianDataInputStream(
          pointBufferedStream);
      int rows = weightBlock.getBlockRows();
      int cols = weightBlock.getMatrixCols();
      short []data = new short[rows * cols];
      int index = 0;
      int size = weightBlock.getStart() * weightBlock.getMatrixCols();
      for (int i = 0; i < size; i++) {
        pointStream.readShort();
      }
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          data[index++] = pointStream.readShort();
        }
      }
      weightBlock.setData(data);
    }
  }

  private static void readFile(String weightFile, boolean isBigEndian, ShortMatrixBlock weightBlock) throws Exception {
    // now read the data from the file
    try (
        BufferedInputStream pointBufferedStream = new BufferedInputStream(
            Files.newInputStream(Paths.get(weightFile),StandardOpenOption.READ)))
    {
      DataInput pointStream = isBigEndian ? new DataInputStream(
          pointBufferedStream) : new LittleEndianDataInputStream(
          pointBufferedStream);
      int rows = weightBlock.getBlockRows();
      int cols = weightBlock.getMatrixCols();
      short []data = new short[rows * cols];
      int index = 0;
      int size = weightBlock.getStart() * weightBlock.getMatrixCols();
      for (int i = 0; i < size; i++) {
        pointStream.readShort();
      }
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
          data[index++] = pointStream.readShort();
        }
      }
      weightBlock.setData(data);

    }
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
