package edu.iu.dsc.flink.damds.types;

public abstract class Timing {
  protected long startTime;
  protected long endTime;
  protected int itr;

  public void setItr(int itr) {
    this.itr = itr;
  }

  public void start() {
    startTime = System.currentTimeMillis();
  }

  public void end() {
    endTime = System.currentTimeMillis();
  }

  abstract public String serialize();
}
