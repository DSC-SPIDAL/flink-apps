package edu.iu.dsc.flink.io;

import com.google.common.base.Strings;
import edu.indiana.soic.spidal.common.TransformationFunction;
import org.apache.flink.util.SplittableIterator;

import java.util.Iterator;

public class MatrixIterator extends SplittableIterator<RowBlock> {
    private String distanceFile;
    private String weightsFile;

    private int globalColCount;

    private boolean isBigEndian;
    boolean isSimpleWeights = false;
    boolean isConstantWeight = false;
    private boolean isSammon;

    private int repetitions;
    private TransformationFunction dTrans;
    private TransformationFunction wTrans;

    private int rowStart;
    private int numRows;
    private int numSplits;
    private int id;

    private boolean isRead = false;

    public MatrixIterator(String distanceFile, String weightsFile, int globalColCount,
                          boolean isBigEndian, boolean isSimpleWeights, boolean isSammon,
                          int repetitions, TransformationFunction dTrans,
                          TransformationFunction wTrans){
        this(distanceFile, weightsFile, globalColCount,
            isBigEndian, isSimpleWeights, isSammon, repetitions, dTrans, wTrans, -1, -1, 1, 0);
    }
    private MatrixIterator(
        String distanceFile, String weightsFile, int globalColCount,
        boolean isBigEndian, boolean isSimpleWeights, boolean isSammon,
        int repetitions, TransformationFunction dTrans,
        TransformationFunction wTrans, int rowStart, int numRows, int numSplits, int id) {
        System.out.println("****Matrix Iterator Constructor Call");
        this.distanceFile = distanceFile;
        this.weightsFile = weightsFile;
        this.globalColCount = globalColCount;
        this.isBigEndian = isBigEndian;
        this.isSimpleWeights = isSimpleWeights;
        this.isSammon = isSammon;
        this.repetitions = repetitions;
        this.dTrans = dTrans;
        this.wTrans = wTrans;

        this.rowStart = rowStart;
        this.numRows = numRows;
        this.numSplits = numSplits;
        this.id = id;

        isConstantWeight = Strings.isNullOrEmpty(weightsFile);
    }

    @Override
    public Iterator<RowBlock>[] split(int numPartitions) {
//        System.out.println("***Split call");
        int q = globalColCount / numPartitions;
        int r = globalColCount % numPartitions;

        int rowStart = 0,numRows;
        Iterator<RowBlock>[] iterators = new MatrixIterator[numPartitions];
        for (int i = 0; i < numPartitions; ++i){
            numRows = (q+(i < r ? 1 : 0));
            iterators[i] = new MatrixIterator(
                distanceFile, weightsFile, globalColCount, isBigEndian,
                isSimpleWeights, isSammon, repetitions, dTrans, wTrans,
                rowStart, numRows, numPartitions, i);
            rowStart+=numRows;
        }

        return iterators;
    }

    @Override
    public int getMaximumNumberOfSplits() {
        return numSplits;
    }

    @Override
    public boolean hasNext() {
        return !isRead;
    }

    @Override
    public RowBlock next() {
        RowBlock rb = new RowBlock(id, globalColCount, distanceFile, weightsFile, rowStart, numRows, isBigEndian,isSimpleWeights, dTrans, wTrans, repetitions);
        isRead = true;
        return rb;
    }

    public Iterator<RowBlock> getSplit(int num, int numPartitions) {
        if (numPartitions < 1 || num < 0 || num >= numPartitions) {
            throw new IllegalArgumentException();
        }

        return split(numPartitions)[num];
    }
}
