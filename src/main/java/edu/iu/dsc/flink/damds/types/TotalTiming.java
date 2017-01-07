package edu.iu.dsc.flink.damds.types;

import java.util.ArrayList;
import java.util.List;

public class TotalTiming extends Timing {
  private List<LoopTiming> loopTimings = new ArrayList<>();

  public void add(LoopTiming timing) {
    loopTimings.add(timing);
  }

  @Override
  public String serialize() {
    StringBuilder sb = new StringBuilder();
    sb.append("MDS: ").append("\n");
    sb.append("start: ").append(startTime).append("\n");
    sb.append("end: ").append(endTime).append("\n\n");
    sb.append("Total temp loops: ").append(loopTimings.size()).append("\n");
    for (LoopTiming s : loopTimings) {
      sb.append(s.serialize()).append("\n\n");
    }
    sb.append("Total time: ").append((endTime - startTime)).append("\n");
    sb.append("End Loop: ").append(itr);
    return sb.toString();
  }
}
