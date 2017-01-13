package edu.iu.dsc.flink.collectives;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

public class Program {
  public static void main(String[] args) throws Exception {
    // Checking input parameters
    final ParameterTool params = ParameterTool.fromArgs(args);
    // set up execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setGlobalJobParameters(params);
    int size = params.getInt("size", 1000);
    int itr = params.getInt("itr", 10);
    String out = params.get("out", size + "_" + itr + "_" + env.getParallelism());
    System.out.println(String.format("Using size %d and itr %d", size, itr));
    int coll = params.getInt("col", 0);
    if (coll == 0) {
      System.out.println("******************** Reduce ********************");
      Reduce reduce = new Reduce(size, itr, env, out);
      reduce.execute();
    } else if (coll == 1) {
      System.out.println("******************** All Reduce ********************");
      AllReduce allReduce = new AllReduce(size, itr, env, out);
      allReduce.execute();
    }

    env.execute();
  }
}
