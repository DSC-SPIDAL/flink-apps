package edu.iu.dsc.flink.mm;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class MatrixInputFormat<T> extends FileInputFormat<T> {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory
      .getLogger(MatrixInputFormat.class);

  protected boolean isBigEndian = true;
  protected int globalColumnCount;
  protected int globalRowCount;
  protected boolean isRead = false;
  protected int byteSize = Double.BYTES;
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
      length = (q + (i < r ? 1 : 0)) * globalColumnCount * byteSize;
      FileInputSplit fis = new FileInputSplit(i, this.filePath, start, length, blocks[0].getHosts());
      splits[i] = fis;
      start += length;
    }

    numSplits = minNumSplits;
    LOG.info("No of splits: " + numSplits);
    return splits;
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return isRead;
  }

  @Override
  public void open(FileInputSplit fileSplit) throws IOException {
    // This uses an input stream, later see how to change to
    // memory maps, will have to change nextRecord() method as well
    super.open(fileSplit);
    isRead = false;
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
