package edu.iu.dsc.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.ParallelIteratorInputFormat;
import edu.iu.dsc.flink.io.MatrixIterator;
import edu.iu.dsc.flink.io.RowBlock;
import edu.iu.dsc.flink.io.RowBlockType;

public class IteratorTest {
    public static void main(String[] args) throws Exception {

        /*CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                                                       .withCache(
                                                           "preConfigured",
                                                           CacheConfigurationBuilder
                                                               .newCacheConfigurationBuilder(
                                                                   Long.class,
                                                                   MyClass
                                                                       .class)
                                                               .build())
                                                       .build(true);

        Cache<Long, MyClass> preConfigured
            = cacheManager.getCache("preConfigured", Long.class, MyClass.class);

        Cache<Long, MyClass> myCache = cacheManager.createCache("myCache",
            CacheConfigurationBuilder
                .newCacheConfigurationBuilder(Long.class, MyClass.class).build());

        IntStream.range(0,3).forEach(i -> new MyClass(i, String.valueOf(i)));
        myCache.put(1L, new MyClass(2, "adad"));
        MyClass v = myCache.get(1L);
        System.out.println(v.getId() + " "  +v.getName());*/


        final ExecutionEnvironment
            env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String binaryFile = args[0];
        int globalColCount = Integer.parseInt(args[1]);
        boolean isBigEndian = Boolean.parseBoolean(args[2]);
        boolean divideByShortMax = Boolean.parseBoolean(args[3]);

        MatrixIterator mitr = new MatrixIterator(binaryFile, null, globalColCount, true, false, false, 1, null, null);
        ParallelIteratorInputFormat<RowBlock> pitrIf = new ParallelIteratorInputFormat<>(mitr);

        DataSet<RowBlock>
            ds = env.createInput(pitrIf, new RowBlockType());
        ds.map(rb -> {
//            myCache.put(1L, rb);
//            RowBlock value = myCache.get(1L);
//            System.out.println("******************************" + value.getId());

            final int id = rb.getId();
            rb.open();
            int rowStart = rb.getRowStart();
            int numRows = rb.getNumRows();
            String str = "ID: " + id + "\n";
            for (int i = rowStart; i < numRows+rowStart; ++i){
                for (int j = 0; j < globalColCount; ++j) {
                    str += Math.round(rb.getDistance(i, j)*(globalColCount*globalColCount)) + " ";
                }
                str += "\n";
            }
            System.out.println(str);
            return id;
        }).collect();

        System.out.println(ds.count());



//        cacheManager.close();

    }
}
