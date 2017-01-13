package edu.iu.dsc.flink.collectives;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public abstract class Collective {
  int size;
  int iterations;
  ExecutionEnvironment env;
  String outFile;

  public Collective(int size, int iterations, ExecutionEnvironment env, String outFile) {
    this.size = size;
    this.iterations = iterations;
    this.env = env;
    this.outFile = outFile;
  }

  public DataSet<CollectiveData> loadDataSet(int size, ExecutionEnvironment env) {
    int p =  env.getParallelism();
//    List<CollectiveData> list = new ArrayList<>();
//    for (int j = 0; j < p; j++) {
      List<Integer> data = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        data.add(i);
      }

      CollectiveData collectiveData = new CollectiveData(data, System.currentTimeMillis());
//      list.add(collectiveData);
//    }
    return env.fromElements(collectiveData);
  }

  public DataSet<Integer> loadMapDataSet(int size, ExecutionEnvironment env) {
    int p =  env.getParallelism();
    List<Integer> list = new ArrayList<>();
    for (int j = 0; j < p; j++) {
      list.add(j);
    }
    return env.fromCollection(list);
  }

  public abstract void execute();
}
