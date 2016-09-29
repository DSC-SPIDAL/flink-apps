package edu.iu.dsc.flink.damds.types;

public class StressTiming extends Timing {
  private int cgIterators;



  public void setCgIterators(int cgIterators) {
    this.cgIterators = cgIterators;
  }

  public String serialize() {
    StringBuilder sb = new StringBuilder();
    sb.append("Stress: ").append(itr).append("\n");
    sb.append("start: ").append(startTime).append("\n");
    sb.append("end: ").append(endTime).append("\n");
    sb.append("CG iterations: ").append(cgIterators).append("\n");
    sb.append("Stress time: ").append((endTime - startTime)).append("\n");
    sb.append("End Stress: ").append(itr);
    return sb.toString();
  }
}
