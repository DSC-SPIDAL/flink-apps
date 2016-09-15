package edu.iu.dsc.flink.damds.types;

public class Iteration {
  public double tCur;
  public double stress;
  public double preStress;
  public double tMin;
  public int stressItr;
  public int tItr;

  public Iteration() {
  }

  public Iteration(double tCur, double stress, double tMin, double preStress, int stressItr, int tItr) {
    this.tCur = tCur;
    this.stress = stress;
    this.preStress = preStress;
    this.stressItr = stressItr;
    this.tItr = tItr;
    this.tMin = tMin;
  }

  public String save() {
    return toString();
  }

  public void load(String iteration) {
    String []split = iteration.trim().split(",");
    if (split.length != 6) {
      throw new RuntimeException("Failed to read");
    }
    tCur = Double.valueOf(split[0]);
    stress = Double.valueOf(split[1]);
    stressItr = Integer.valueOf(split[2]);
    tItr = Integer.valueOf(split[3]);
    preStress = Double.valueOf(split[4]);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(tCur).append(",").append(stress).append(",").
        append(stressItr).append(",").append(tItr).append(",").append(preStress).append(",").append(tMin);
    return builder.toString();
  }
}
