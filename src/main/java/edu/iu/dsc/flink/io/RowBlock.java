package edu.iu.dsc.flink.io;

import com.google.common.base.Strings;
import edu.indiana.soic.spidal.common.*;

import java.io.Serializable;
import java.nio.ByteOrder;

public class RowBlock implements Serializable {
    static double INV_SHORT_MAX = 1.0 / Short.MAX_VALUE;

    private final int id;

    private final String distanceFile;
    private final String weightsFile;

    private final int globalColCount;
    private final int rowStart;
    private final int numRows;

    private final boolean isBigEndian;
    boolean isSimpleWeights = false;
    boolean isConstantWeight = false;
    private boolean isSammon;

    private final int repetitions;
    private final TransformationFunction dTrans;
    private final TransformationFunction wTrans;

    short[] distances;
    short[] weights;
    double[] simpleWeights;
    double avgDistance;
    double sammonFactor;

    boolean isReady = false;

    public RowBlock(int id,
        int globalColCount, String distanceFile, String weightsFile,
        int rowStart, int numRows, boolean isBigEndian) {
        this(id,globalColCount, distanceFile, weightsFile, rowStart, numRows,
            false, isBigEndian, 1);
    }

    public RowBlock(int id,
        int globalColCount, String distanceFile, String weightsFile,
        int rowStart, int numRows, boolean isBigEndian, int repetitions) {
        this(id,globalColCount, distanceFile, weightsFile, rowStart, numRows, isBigEndian,
            false, repetitions);
    }

    public RowBlock(int id,
        int globalColCount, String distanceFile, String weightsFile,
        int rowStart, int numRows, boolean isBigEndian, boolean isSimpleWeights, int repetitions) {
        this(id,globalColCount, distanceFile, weightsFile, rowStart, numRows,
            isBigEndian, isSimpleWeights, null, null, repetitions);
    }

    public RowBlock(int id,
        int globalColCount, String distanceFile, String weightsFile,
        int rowStart, int numRows, boolean isBigEndian, boolean isSimpleWeights,
        TransformationFunction dTrans, TransformationFunction wTrans,
        int repetitions) {

        this.id = id;
        this.globalColCount = globalColCount;
        this.distanceFile = distanceFile;
        this.weightsFile = weightsFile;
        this.rowStart = rowStart;
        this.numRows = numRows;
        this.isBigEndian = isBigEndian;
        this.isSimpleWeights = isSimpleWeights;
        this.dTrans = dTrans;
        this.wTrans = wTrans;
        this.repetitions = repetitions;

        open();

    }

    public void open() {
        int elementCount = numRows * globalColCount;
        distances = new short[elementCount];

        ByteOrder byteOrder = (isBigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        BinaryReader1D
            .readRowRange(distanceFile, new Range(rowStart, (rowStart+numRows) - 1),
                globalColCount, byteOrder,
                true, dTrans, repetitions, distances);

        if (!Strings.isNullOrEmpty(weightsFile)){
            if (!isSimpleWeights) {
                weights = new short[elementCount];
                BinaryReader1D.readRowRange(weightsFile,
                    new Range(rowStart, numRows - 1), globalColCount,
                    byteOrder, true, wTrans, repetitions, weights);
            } else {
                simpleWeights = BinaryReader2D
                    .readSimpleFile(weightsFile, globalColCount);
            }
        } else {
            isConstantWeight = true;
        }
        isReady = false;
    }

    public void useSammonWeights(double avgDistance){
        useSammonWeights(avgDistance, 0.001);
    }
    public void useSammonWeights(double avgDistance, double sammonFactor){
        isSammon = true;
        this.avgDistance = avgDistance;
        this.sammonFactor = sammonFactor;
    }


    public double getDistance(int globalRow, int globalCol){
        return distances[(globalRow-rowStart) * globalColCount + globalCol] *
               INV_SHORT_MAX;
    }

    public double getWeight(int globalRow, int globalCol){
        double w = 1.0;
        if (!isConstantWeight) {
            if (isSimpleWeights) {
                w = (wTrans == null)
                    ? simpleWeights[globalRow] * simpleWeights[globalCol]
                    : wTrans.transform(simpleWeights[globalRow] * simpleWeights[globalCol]);
            }
            else {
                w = weights[(globalRow - rowStart) * globalColCount + globalCol]
                    * INV_SHORT_MAX;

            }
        }
        return isSammon ? getSammonWeight(w, globalRow, globalCol) : w;
    }

    private double getSammonWeight(double w, int globalRow, int globalCol){
        double d = getDistance(globalRow, globalCol);
        return w / Math.max(d, sammonFactor * avgDistance);
    }

    public int getId() {
        return id;
    }

    public int getGlobalColCount() {
        return globalColCount;
    }

    public int getRowStart() {
        return rowStart;
    }

    public int getNumRows() {
        return numRows;
    }


}
