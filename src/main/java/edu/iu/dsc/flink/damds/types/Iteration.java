package edu.iu.dsc.flink.damds.types;

public class Iteration {
  public double tCur;
  public double stress;
  public int stressItr;
  public int tItr;

  public Iteration(double tCur, double stress, int stressItr, int tItr) {
    this.tCur = tCur;
    this.stress = stress;
    this.stressItr = stressItr;
    this.tItr = tItr;
  }
}
