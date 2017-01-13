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
    System.out.println(String.format("Using size %d and itr %d", size, itr));
    Reduce reduce = new Reduce(size, itr, env);
    reduce.execute();

    env.execute();
  }
}
