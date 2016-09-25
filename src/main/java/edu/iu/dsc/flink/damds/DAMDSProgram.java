package edu.iu.dsc.flink.damds;

import com.google.common.base.Optional;
import edu.iu.dsc.flink.damds.configuration.ConfigurationMgr;
import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import edu.iu.dsc.flink.mm.Utils;
import org.apache.commons.cli.*;
import org.apache.flink.api.java.ExecutionEnvironment;

public class DAMDSProgram {
  public static int BlockSize;
  private static Options programOptions = new Options();

  static {
    programOptions.addOption(
            String.valueOf(Constants.CMD_OPTION_SHORT_C),
            Constants.CMD_OPTION_LONG_C, true,
            Constants.CMD_OPTION_DESCRIPTION_C);
    programOptions.addOption(Utils.createOption("dFile", true, "distance file", false));
    programOptions.addOption(Utils.createOption("wFile", true, "weight file", false));
    programOptions.addOption(Utils.createOption("pFile", true, "point file", false));
    programOptions.addOption(Utils.createOption("points", true, "no of points", false));
    programOptions.addOption(Utils.createOption("outFolder", true, "Out folder", false));
  }

  public static void main(String[] args) throws Exception {
    Optional<CommandLine> parserResult =
            parseCommandLineArguments(args, programOptions);
    CommandLine cmd = parserResult.get();

    DAMDSSection config = readConfiguration(cmd);
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DAMDS damds = new DAMDS(config, env);
    damds.execute();
  }

  private static DAMDSSection readConfiguration(CommandLine cmd) {
    DAMDSSection config = ConfigurationMgr.LoadConfiguration(
        cmd.getOptionValue(Constants.CMD_OPTION_LONG_C)).damdsSection;

    if (cmd.getOptionValue("dFile") != null) {
      config.distanceMatrixFile = cmd.getOptionValue("dFile");
    }

    if (cmd.getOptionValue("wFile") != null) {
      config.weightMatrixFile = cmd.getOptionValue("wFile");
    }

    if (cmd.getOptionValue("pFile") != null) {
      config.pointsFile = cmd.getOptionValue("pFile");
    }

    if (cmd.getOptionValue("points") != null) {
      config.numberDataPoints = Integer.parseInt(cmd.getOptionValue("points"));
    }

    if (cmd.getOptionValue("outFolder") != null) {
      config.outFolder = cmd.getOptionValue("outFolder");
    }

    BlockSize = config.blockSize;
    return config;
  }

  /**
   * Parse command line arguments
   *
   * @param args Command line arguments
   * @param opts Command line options
   * @return An <code>Optional&lt;CommandLine&gt;</code> object
   */
  private static Optional<CommandLine> parseCommandLineArguments(
          String[] args, Options opts) {

    CommandLineParser optParser = new GnuParser();

    try {
      return Optional.fromNullable(optParser.parse(opts, args));
    }
    catch (ParseException e) {
      e.printStackTrace();
    }
    return Optional.fromNullable(null);
  }
}
