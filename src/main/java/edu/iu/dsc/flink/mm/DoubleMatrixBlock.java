package edu.iu.dsc.flink.mm;

public class DoubleMatrixBlock extends MatrixBlock {
  // the actual data for this block
  public double []data;

  public DoubleMatrixBlock() {
  }

  public DoubleMatrixBlock(int index, int start, int blockRows, int matrixCols, int matrixRows) {
    super(index, start, blockRows, matrixCols, matrixRows);
  }

  public void setData(double[] data) {
    this.data = data;
  }

  public double[] getData() {
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
