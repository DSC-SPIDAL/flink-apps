package edu.iu.dsc.flink;

import edu.indiana.soic.spidal.common.DoubleStatistics;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import edu.iu.dsc.flink.io.SMatrixInputFormat;

import java.util.List;

public class Statistics {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        /*DataSet<String> text = env.readTextFile("src/main/resources/sample.txt");
        System.out.println(text.count());*/

//        String binaryFile = "src/main/resources/sample.bin";
        String binaryFile = args[0];
        /*SerializedInputFormat<ShortValue> sif = new SerializedInputFormat<>();
        sif.setFilePath(binaryFile);
        DataSet<ShortValue> ds = env.createInput(sif,
            ValueTypeInfo.SHORT_VALUE_TYPE_INFO);
        System.out.println(ds.count());*/

        int globalColCount = Integer.parseInt(args[1]);
        boolean isBigEndian = Boolean.parseBoolean(args[2]);
        boolean divideByShortMax = Boolean.parseBoolean(args[3]);
        double factor = divideByShortMax ? 1.0/Short.MAX_VALUE : 1.0;
        SMatrixInputFormat smif = new SMatrixInputFormat();
        smif.setFilePath(binaryFile);
        smif.setBigEndian(isBigEndian);
        smif.setGlobalColumnCount(globalColCount);
        DataSet<Short[]> ds = env.createInput(smif, BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO);

//        System.out.println(ds.count());

//        MapOperator<Short[], DoubleStatistics> op = ds.map(arr -> {
        DataSet<DoubleStatistics> op = ds.map(arr -> {

            DoubleStatistics stats = new DoubleStatistics();
            for (int i = 0; i < arr.length; ++i){
                stats.accept(arr[i]*factor);
            }

            /*for (int i = 0; i < arr.length; ++i){
                System.out.print(arr[i] + " ");
                if ((i+1)%globalColCount == 0){
                    System.out.println();
                }
            }*/
            return stats;
        });

        List<DoubleStatistics>
            result = op.reduce((a, b)-> {a.combine(b); return a;}).collect();
        System.out.println(result.get(0));
//        System.out.println(op.count());
//        System.out.println(ds.count());
    }
}
