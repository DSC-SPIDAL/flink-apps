package edu.iu.dsc.flink.mm;

public class ShortMatrixBlock extends MatrixBlock {
  // the actual data for this block
  private short []data;

  public void setData(short[] data) {
    this.data = data;
  }

  public short[] getData() {
    return data;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < blockRows; i++) {
      for (int j = 0; j < matrixCols; j++) {
        sb.append(data[i * matrixCols + j]).append(" ");
      }
      sb.append("\n");
    }
    return sb.toString();
  }
}
