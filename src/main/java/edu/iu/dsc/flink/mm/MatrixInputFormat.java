package edu.iu.dsc.flink.mm;


import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.*;
import org.apache.flink.hadoop.shaded.com.google.common.io.LittleEndianDataInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;

public class MatrixInputFormat extends FileInputFormat<MatrixBlock> {
  private static final long serialVersionUID = 1L;

  /**
   * The log.
   */
  private static final Logger LOG = LoggerFactory
      .getLogger(MatrixInputFormat.class);

  private boolean isBigEndian = true;
  private int globalColumnCount;
  private int globalRowCount;

  private boolean isRead = false;

  @Override
  public FileInputSplit[] createInputSplits(int minNumSplits)
      throws IOException {
    final FileSystem fs = this.filePath.getFileSystem();
    final FileStatus file = fs.getFileStatus(this.filePath);

    LOG.info("Min splits: " + minNumSplits);

    FileInputSplit[] splits = new FileInputSplit[minNumSplits];
    int q = globalRowCount / minNumSplits;
    int r = globalRowCount % minNumSplits;

    long start = 0, length;
    BlockLocation[] blocks;
    for (int i = 0; i < minNumSplits; ++i) {
      blocks = fs.getFileBlockLocations(file, 0, file.getLen());
      if (blocks.length != 1) {
        throw new RuntimeException("File blocks should be 1 for local file system");
      }
      length = (q + (i < r ? 1 : 0)) * globalColumnCount * Double.BYTES;
      FileInputSplit fis = new FileInputSplit(i, this.filePath, start, length, blocks[0].getHosts());
      splits[i] = fis;
      start += length;
    }
    return splits;
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return isRead;
  }

  @Override
  public MatrixBlock nextRecord(MatrixBlock block) throws IOException {
    long splitLength = getSplitLength();
    int rows = (int) (splitLength / (Double.BYTES * globalColumnCount));
    int splitIndex = this.currentSplit.getSplitNumber();
    LOG.info("{} Split Length: {}\n", splitIndex, splitLength);
    int length = (int)(this.splitLength / Double.BYTES);
    block = new MatrixBlock();

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

    LOG.info("Block print: " + splitIndex + "->" + block.toString());

    return block;
  }

  @Override
  public void open(FileInputSplit fileSplit) throws IOException {
    // This uses an input stream, later see how to change to
    // memory maps, will have to change nextRecord() method as well
    super.open(fileSplit);
  }

  private void throwExceptionIfDistributedFS() {
    try {
      if (this.filePath.getFileSystem().isDistributedFS()){
        throw new IllegalArgumentException("Distributed file systems are not supported in " + this.getClass().getSimpleName());
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  public boolean isBigEndian() {
    return isBigEndian;
  }

  public void setBigEndian(boolean bigEndian) {
    isBigEndian = bigEndian;
  }

  public int getGlobalColumnCount() {
    return globalColumnCount;
  }

  public void setGlobalColumnCount(int globalColumnCount) {
    this.globalColumnCount = globalColumnCount;
  }

  public int getGlobalRowCount() {
    return globalRowCount;
  }

  public void setGlobalRowCount(int globalRowCount) {
    this.globalRowCount = globalRowCount;
  }
}
