package edu.iu.dsc.flink.kmeans;

import java.io.Serializable;

public class Centroid2 extends Point2 implements Serializable {
  public int id;

  public Centroid2(int id, double[] values) {
    super(values);
    this.id = id;
  }

  public Centroid2(int id, Point2 p) {
    super(p.values);
    this.id = id;
  }

  @Override
  public String toString() {
    return id + " " + super.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Centroid2 centroid = (Centroid2) o;
    return id == centroid.id;
  }

  @Override
  public int hashCode() {
    return id;
  }
}
