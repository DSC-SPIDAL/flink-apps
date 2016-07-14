package edu.iu.dsc.flink.mm;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.Files;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class MatrixMultiply {
  private static final Logger LOG = LoggerFactory
      .getLogger(MatrixMultiply.class);

  private final static String outFile = "/home/supun/dev/projects/dsspidal/flink_mm/flink-mm-git/out.output";
  private final static String filePath = "/home/supun/dev/projects/dsspidal/flink_mm/flink-mm-git/out.input";

  public static void main(String[] args) throws Exception {
    Options programOptions = new Options();
    programOptions.addOption("n", true, "nxm matrix A");
    programOptions.addOption("m", true, "nxm matrix A");
    programOptions.addOption("p", true, "mxp matrix B");
    programOptions.addOption("i", true, "Input File name");
    programOptions.addOption("o", true, "Input File name");
    programOptions.addOption("t", false, "Testing mode");
    Option option = new Option("tf", true, "Test ");
    option.setRequired(false);
    programOptions.addOption(option);

    Optional<CommandLine> parserResult = Utils.parseCommandLineArguments(args, programOptions);
    if (!parserResult.isPresent()) {
      new HelpFormatter().printHelp("MM", programOptions);
      return;
    }

    CommandLine cmd = parserResult.get();
    int n = Integer.parseInt(cmd.getOptionValue("n"));
    int m = Integer.parseInt(cmd.getOptionValue("m"));
    int p = Integer.parseInt(cmd.getOptionValue("p"));
    String inputFileName = cmd.getOptionValue("i");
    String outputFileName = cmd.getOptionValue("o");
    boolean testMode = cmd.hasOption("t");
    String testFile = cmd.getOptionValue("tf");

    // delete the out file
    File deleteFile = new File(outFile);
    if (deleteFile.exists()) {
      deleteFile.delete();
    }

    // first generate the input matrix and write it
    MatrixFileGenerator.writeMatrixFile(n, m, true, inputFileName);

    // now generate the B matrix
    Matrix matrixB = new Matrix();
    matrixB.rows = n;
    matrixB.cols = p;
    int matrixBdataSize = matrixB.cols * n;
    matrixB.data = new double[matrixBdataSize];
    Random random = new Random();
    for (int i = 0; i < matrixBdataSize; i++) {
      matrixB.data[i] = random.nextDouble();
    }

    if(!testMode) {
      matrixMultiply(n, matrixB, inputFileName, outputFileName);
    } else {
      double[] aData = MatrixFileGenerator.readMatrixFile(inputFileName, n, m, true);
      MatrixBlock aBlock = new MatrixBlock(0, 0, n, m, n);
      aBlock.data = aData;
      // now multiply normally
      double c[] = multiply(aBlock, matrixB);
      Matrix cMatrix = new Matrix(n, p, false);
      cMatrix.data = c;
      Files.write(cMatrix.toString(), new File(testFile), Charsets.UTF_8);
      // now multiply using flink
      matrixMultiply(n, matrixB, inputFileName, outputFileName);
    }
  }

  private static void matrixMultiply(int matrixRows, Matrix matrixB, String inputFile, String outFile) throws Exception {
    // setup the custom input format for the matrix
    MatrixInputFormat inputFormat = new MatrixInputFormat();
    inputFormat.setBigEndian(true);
    inputFormat.setGlobalColumnCount(matrixRows);
    inputFormat.setGlobalRowCount(matrixB.rows);

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<MatrixBlock> blockDataSet = env.readFile(inputFormat, inputFile);
    //DataSet<MatrixBlock> blockDataSet2 = env.fromCollection(matrixABlocks);
    DataSet<Matrix> matrixDataSet = env.fromElements(matrixB);

    blockDataSet.map(new RichMapFunction<MatrixBlock, MatrixBlock>() {
      @Override
      public MatrixBlock map(MatrixBlock matrixABlock) throws Exception {
        List<Matrix> matrix = getRuntimeContext().getBroadcastVariable("single_matrix");
        Matrix matrixB = matrix.get(0);
        // System.out.println("Multiply: " + matrixB.toString());
        System.out.format("Multiply: (%d) = %s\n", matrixABlock.index, matrixABlock.toString());
        int cDataSize = matrixB.cols *  matrixABlock.blockRows;
        double []C = new double[cDataSize];
        Utils.matrixMultiply(matrixABlock.data, matrixB.data, matrixABlock.matrixRows, matrixABlock.matrixCols, matrixB.cols, matrixABlock.blockRows, C);

        MatrixBlock b = new MatrixBlock();
        b.data = C;
        b.index = matrixABlock.index;
        b.blockRows = matrixABlock.blockRows;
        b.matrixRows = matrixABlock.matrixRows;
        b.matrixCols = matrixB.cols;
        // start of this block calculated using the A's cols
        b.start = matrixABlock.start;

        System.out.format("After multiply: (%d) = %s\n", matrixABlock.index, b.toString());

        return b;
      }
    }).withBroadcastSet(matrixDataSet, "single_matrix").reduceGroup(new GroupReduceFunction<MatrixBlock, Matrix>() {
      @Override
      public void reduce(Iterable<MatrixBlock> iterable, Collector<Matrix> collector) throws Exception {
        Matrix m = new Matrix();
        m.setColumnMajor(false);

        boolean init = false;
        Iterator<MatrixBlock> b = iterable.iterator();
        while (b.hasNext()) {
          MatrixBlock matrixBlock = b.next();
          if (!init) {
            m.data = new double[matrixBlock.matrixCols * matrixBlock.matrixRows];
            m.rows = matrixBlock.matrixRows;
            m.cols = matrixBlock.matrixCols;
            init = true;
          }
          System.out.format("Reduce matrix index: %d = %s\n", matrixBlock.index, matrixBlock.toString());
          System.out.format("m size=(%d X %d) block is=%d block rows=%d\n", m.rows, m.cols, matrixBlock.index, matrixBlock.blockRows);
          System.out.format("index is=%d lenght is=%d\n", matrixBlock.index * matrixBlock.blockRows, matrixBlock.blockRows * matrixBlock.matrixCols);
          System.arraycopy(matrixBlock.data, 0, m.data, matrixBlock.start * m.cols, matrixBlock.blockRows * matrixBlock.matrixCols);
        }
        collector.collect(m);
      }
    }).writeAsText(outFile);

    env.execute();
  }

  private static void multiply(List<MatrixBlock> matrixABlocks, Matrix matrixB) {
    for (int i = 0; i < matrixABlocks.size(); i++) {
      MatrixBlock matrixABlock = matrixABlocks.get(i);
      int cDataSize = matrixB.cols * matrixABlock.blockRows;
      double[] C = new double[cDataSize];
      Utils.matrixMultiply(matrixABlock.data, matrixB.data, matrixABlock.matrixRows, matrixABlock.matrixCols, matrixB.cols, matrixABlock.blockRows, C);

      Matrix cBlock = new Matrix(matrixABlock.blockRows, matrixB.cols);
      cBlock.data = C;
      System.out.println(cBlock);
    }
  }

  private static double[] multiply(MatrixBlock matrixABlocks, Matrix matrixB) {
    int cDataSize = matrixB.cols * matrixABlocks.blockRows;
    double[] C = new double[cDataSize];
    Utils.matrixMultiply(matrixABlocks.data, matrixB.data, matrixABlocks.matrixRows, matrixABlocks.matrixCols, matrixB.cols, matrixABlocks.blockRows, C);

    return C;
  }
}
