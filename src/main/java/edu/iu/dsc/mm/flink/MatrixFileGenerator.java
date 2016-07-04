package edu.iu.dsc.mm.flink;

import com.google.common.base.Optional;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class MatrixFileGenerator {

  private static Options programOptions = new Options();

  static {
    programOptions.addOption("n", true, "N");
    programOptions.addOption("m", true, "M");
    programOptions.addOption("f", true, "File name");
  }

  public static void main(String[] args) throws IOException {
    Optional<CommandLine> parserResult = Utils.parseCommandLineArguments(args, programOptions);
    if (!parserResult.isPresent()) {
      new HelpFormatter().printHelp("Datagenerator", programOptions);
      return;
    }

    CommandLine cmd = parserResult.get();
    int n = Integer.parseInt(cmd.getOptionValue("n"));
    int m = Integer.parseInt(cmd.getOptionValue("m"));
    String fileName = cmd.getOptionValue("f");
    double []data = new double[n * m];
    for (int i = 0; i < n * m; i++) {
      data[i] = Math.random();
    }
    writeMatrixFile(n, m, data, true, fileName);
  }

  public static void writeMatrixFile(
      int n, int m, boolean isBigEndian, String outFile)
      throws IOException {
    Path pointsFile = Paths.get(outFile);
    Random random = new Random();
    try (
        BufferedOutputStream pointBufferedStream = new BufferedOutputStream(
            Files.newOutputStream(pointsFile, StandardOpenOption.CREATE)))
    {
      DataOutput pointStream = isBigEndian ? new DataOutputStream(
          pointBufferedStream) : new LittleEndianDataOutputStream(
          pointBufferedStream);
      for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
          pointStream.writeDouble(random.nextDouble());
        }
      }
    }
  }

  public static void writeMatrixFile(
      int n, int m, double []data, boolean isBigEndian, String outFile)
      throws IOException {
    Path pointsFile = Paths.get(outFile);
    try (
        BufferedOutputStream pointBufferedStream = new BufferedOutputStream(
            Files.newOutputStream(pointsFile, StandardOpenOption.CREATE)))
    {
      DataOutput pointStream = isBigEndian ? new DataOutputStream(
          pointBufferedStream) : new LittleEndianDataOutputStream(
          pointBufferedStream);
      for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
          pointStream.writeDouble(data[i * m + j]);
        }
      }
    }
  }

  public static double[] readMatrixFile(String fileName, int rows, int cols, boolean isBigEndian) throws IOException {
    Path pointsFile = Paths.get(fileName);
    try (
        BufferedInputStream pointBufferedStream = new BufferedInputStream(
            Files.newInputStream(pointsFile, StandardOpenOption.READ)))
    {
      DataInput pointStream = isBigEndian ? new DataInputStream(
          pointBufferedStream) : new LittleEndianDataInputStream(
          pointBufferedStream);
      double []data = new double[rows * cols];
      int index = 0;
      for (int i = 0; i < rows; i++) {
        for (int j = 0; j < cols; j++) {
           data[index++] = pointStream.readDouble();
        }
      }
      return data;
    }
  }
}
