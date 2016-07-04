package edu.iu.dsc.mm.flink;

import com.google.common.base.Optional;
import com.google.common.io.LittleEndianDataOutputStream;
import org.apache.commons.cli.*;

import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MatrixFileGenerator {

  private static Options programOptions = new Options();

  static {
    programOptions.addOption("n", true, "N");
    programOptions.addOption("m", true, "M");
    programOptions.addOption("f", true, "File name");
  }

  /**
   * Parse command line arguments
   *
   * @param args Command line arguments
   * @param opts Command line options
   * @return An <code>Optional&lt;CommandLine&gt;</code> object
   */
  public static Optional<CommandLine> parseCommandLineArguments(
      String[] args, Options opts) {
    CommandLineParser optParser = new GnuParser();
    try {
      return Optional.fromNullable(optParser.parse(opts, args));
    } catch (ParseException e) {
      System.out.println(e);
    }
    return Optional.fromNullable(null);
  }

  public static void main(String[] args) throws IOException {
    Optional<CommandLine> parserResult = parseCommandLineArguments(args, programOptions);
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
      int n, int m, double []data, boolean isBigEndian, String outFile)
      throws IOException {
    Path pointsFile = Paths.get(outFile);
    try (
        BufferedOutputStream pointBufferedStream = new BufferedOutputStream(
            Files.newOutputStream(pointsFile, StandardOpenOption.CREATE)))
    {
      System.out.println("Is big endian: " + isBigEndian);
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
}
