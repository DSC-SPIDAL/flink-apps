package edu.iu.dsc.flink.mm;

import org.apache.flink.hadoop.shaded.com.google.common.io.LittleEndianDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;

public class DoubleMatrixInputFormat extends MatrixInputFormat<DoubleMatrixBlock> {
  private static final Logger LOG = LoggerFactory
      .getLogger(DoubleMatrixInputFormat.class);

  public DoubleMatrixInputFormat() {
    this.byteSize = Double.BYTES;
  }

  @Override
  public DoubleMatrixBlock nextRecord(DoubleMatrixBlock block) throws IOException {
    long splitLength = getSplitLength();
    int rows = (int) (splitLength / (Double.BYTES * globalColumnCount));
    int splitIndex = this.currentSplit.getSplitNumber();
    LOG.info("{} Split Length: {}\n", splitIndex, splitLength);
    int length = (int)(this.splitLength / Double.BYTES);
    block = new DoubleMatrixBlock();

    block.setStart((int) this.getSplitStart() / (Double.BYTES * globalColumnCount));
    block.setBlockRows(rows);
    block.setIndex(splitIndex);
    block.setMatrixCols(globalColumnCount);
    block.setMatrixRows(globalRowCount);

    double[] reuse = new double[(int) (getSplitLength() / Double.BYTES)];
    if (isBigEndian) {
      DataInputStream dis = new DataInputStream(this.stream);
      for (int i = 0; i < length; ++i) {
        reuse[i] = dis.readDouble();
      }
    } else {
      LittleEndianDataInputStream ldis = new LittleEndianDataInputStream(this.stream);
      for (int i = 0; i < length; ++i) {
        reuse[i] = ldis.readDouble();
      }
    }

    isRead = true;
    block.setData(reuse);
    // LOG.info("Block print: " + splitIndex + "->" + block.toString());
    return block;
  }
}
