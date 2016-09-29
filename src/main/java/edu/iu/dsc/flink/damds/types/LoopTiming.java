package edu.iu.dsc.flink.damds.types;


import java.util.ArrayList;
import java.util.List;

public class LoopTiming extends Timing {
  private List<StressTiming> stressTimings = new ArrayList<>();

  public LoopTiming(int loop) {
    this.itr = loop;
  }

  public void add(StressTiming timing) {
    stressTimings.add(timing);
  }

  public String serialize() {
    StringBuilder sb = new StringBuilder();
    sb.append("Loop: ").append(itr).append("\n");
    sb.append("start: ").append(startTime).append("\n");
    sb.append("end: ").append(endTime).append("\n");
    sb.append("Stress loops: ").append(stressTimings.size()).append("\n");
    for (StressTiming s : stressTimings) {
      sb.append(s.serialize()).append("\n");
    }
    sb.append("Loop time: ").append((endTime - startTime)).append("\n");
    sb.append("End Loop: ").append(itr);
    return sb.toString();
  }
}
