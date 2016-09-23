package edu.iu.dsc.flink.mm;

import com.google.common.base.Strings;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.FileInputSplit;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Pattern;

public class PointInputFormat extends FileInputFormat<Matrix> {
  protected boolean isRead = false;
  protected int rows;
  protected int cols;

  public int getRows() {
    return rows;
  }

  public int getCols() {
    return cols;
  }

  public void setRows(int rows) {
    this.rows = rows;
  }

  public void setCols(int cols) {
    this.cols = cols;
  }

  public void setSpittable(boolean splittable) {
    this.unsplittable = !splittable;
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return isRead;
  }

  @Override
  public Matrix nextRecord(Matrix block) throws IOException {
    DataInputStream dis = new DataInputStream(this.stream);
    int n = rows;
    int m = cols;
    block.setCols(cols);
    block.setRows(rows);
    block.setColumnMajor(false);
    try (Scanner scanner = new Scanner(dis)) {
      Pattern pattern = Pattern.compile("\\s+");
      double[] preX = new double[n * m];
      int row = 0;
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        //process each line in some way
        if (Strings.isNullOrEmpty(line))
          continue; // continue on empty lines - "while" will break on null anyway;

        String[] splits = pattern.split(line.trim());
        for (int i = 0; i < m; ++i) {
          preX[row + i] = Double.parseDouble(splits[i].trim());
        }
        row += m;
      }
      block.setData(preX);
    }
    block.setProperties(new HashMap<String, Object>());
    isRead = true;
    return block;
  }

  @Override
  public void open(FileInputSplit fileSplit) throws IOException {
    // This uses an input stream, later see how to change to
    // memory maps, will have to change nextRecord() method as well
    super.open(fileSplit);
    isRead = false;
  }
}
