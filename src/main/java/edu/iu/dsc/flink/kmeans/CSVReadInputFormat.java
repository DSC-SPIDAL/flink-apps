package edu.iu.dsc.flink.kmeans;

import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.Path;

public class CSVReadInputFormat extends CsvInputFormat<Point> {
  protected CSVReadInputFormat(Path filePath) {
    super(filePath);
  }

  @Override
  protected Point fillRecord(Point point, Object[] objects) {
    double[] values = new double[objects.length];
    for (int i = 0; i < objects.length; i++) {
      values[i] = Double.parseDouble(objects[i].toString());
    }
    return new Point(values);
  }
}
