package edu.iu.dsc.flink.collectives;

import java.io.Serializable;
import java.util.List;

public class CollectiveData implements Serializable {
  public List<Integer> list;

  public long time;

  public long cumulativeTime;

  public CollectiveData(List<Integer> list, long time) {
    this.list = list;
    this.time = time;
  }

  public void addTime(long time) {
    cumulativeTime += time;
  }

  public List<Integer> getList() {
    return list;
  }

  public long getTime() {
    return time;
  }

  public void setList(List<Integer> list) {
    this.list = list;
  }

  public void setTime(long time) {
    this.time = time;
  }

  @Override
  public String toString() {
    return "Time:" + cumulativeTime;
  }
}
