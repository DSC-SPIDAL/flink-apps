package edu.iu.dsc.flink.mm;

import org.apache.flink.hadoop.shaded.com.google.common.io.LittleEndianDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Random;

public class ShortMatrixInputFormat extends MatrixInputFormat<ShortMatrixBlock> {
  private static final Logger LOG = LoggerFactory
      .getLogger(DoubleMatrixInputFormat.class);

  public ShortMatrixInputFormat() {
    this.byteSize = Short.BYTES;
  }

  @Override
  public ShortMatrixBlock nextRecord(ShortMatrixBlock block) throws IOException {
    long splitLength = getSplitLength();
    int rows = (int) (splitLength / (Short.BYTES * globalColumnCount));
    int splitIndex = this.currentSplit.getSplitNumber();
    LOG.info("{} Split Length: {}\n", splitIndex, splitLength);
    int length = (int)(this.splitLength / Short.BYTES);
    block = new ShortMatrixBlock();

    block.setStart((int) this.getSplitStart() / (Short.BYTES * globalColumnCount));
    block.setBlockRows(rows);
    block.setIndex(splitIndex);
    block.setMatrixCols(globalColumnCount);
    block.setMatrixRows(globalRowCount);

    short[] reuse = new short[(int) (getSplitLength() / Short.BYTES)];
    if (!generateData) {
      readFile(length, reuse);
    } else {
      genData(length, reuse);
    }
    LOG.info("Next block for split: " + splitIndex);
    isRead = true;
    block.setData(reuse);
    // LOG.info("Block print: " + splitIndex + "->" + block.toString());
    return block;
  }

  private void readFile(int length, short[] reuse) throws IOException {
    if (isBigEndian) {
      DataInputStream dis = new DataInputStream(this.stream);
      for (int i = 0; i < length; ++i) {
        reuse[i] = dis.readShort();
      }
    } else {
      LittleEndianDataInputStream ldis = new LittleEndianDataInputStream(this.stream);
      for (int i = 0; i < length; ++i) {
        reuse[i] = ldis.readShort();
      }
    }
  }

  private void genData(int length, short[] reuse) throws IOException {
    Random random = new Random();
    short start = (short) random.nextInt(Short.MAX_VALUE);
    for (int i = 0; i < length; ++i) {
      if (start < Short.MAX_VALUE - 1) {
        reuse[i] = (short) (start + 1);
      } else {
        start = 0;
        reuse[i] = 0;
      }
    }
  }
}
