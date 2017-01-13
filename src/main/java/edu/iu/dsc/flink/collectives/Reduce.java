package edu.iu.dsc.flink.collectives;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class Reduce extends Collective {
  public Reduce(int size, int iterations, ExecutionEnvironment env, String outFile) {
    super(size, iterations, env, outFile);
  }

  @Override
  public void execute() {
    DataSet<CollectiveData> data = loadDataSet(size, env);
    IterativeDataSet<CollectiveData> loop = data.iterate(iterations);
    DataSet<Integer> mapSet = loadMapDataSet(size, env);
    DataSet<CollectiveData> dataSet = mapSet.map(new RichMapFunction<Integer, Tuple2<Integer, CollectiveData>>() {
      List<CollectiveData> data;
      int id;
      @Override
      public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        data = getRuntimeContext().getBroadcastVariable("data");
        id = getRuntimeContext().getIndexOfThisSubtask();
      }

      @Override
      public Tuple2<Integer, CollectiveData> map(Integer integer) throws Exception {
        System.out.println(integer);
        for (CollectiveData d : data) {
          d.setTime(System.currentTimeMillis());
          d.hostName = InetAddress.getLocalHost().getHostName();
          return new Tuple2<Integer, CollectiveData>(0, d);
        }
        return null;
      }
    }).withBroadcastSet(loop, "data").groupBy(0).reduceGroup(new GroupReduceFunction<Tuple2<Integer,CollectiveData>, CollectiveData>() {
       @Override
       public void reduce(Iterable<Tuple2<Integer, CollectiveData>> iterable, Collector<CollectiveData> collector) throws Exception {
         String hostName = InetAddress.getLocalHost().getHostName();
         long time = 0;
         List<CollectiveData> list = new ArrayList<CollectiveData>();
        for (Tuple2<Integer, CollectiveData> t : iterable) {
          CollectiveData d = t.f1;
          if (d.hostName.equals(hostName)) {
            time = d.getTime();
          }
          list.add(d);
        }
         long nowTime = System.currentTimeMillis();
        for (CollectiveData d : list) {
          d.addTime(nowTime - time);
          collector.collect(d);
        }
       }
     });

    DataSet<CollectiveData> finalData = loop.closeWith(dataSet);
    finalData.writeAsText(outFile, FileSystem.WriteMode.OVERWRITE);
  }
}
