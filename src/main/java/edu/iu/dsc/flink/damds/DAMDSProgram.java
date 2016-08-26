package edu.iu.dsc.flink.damds;

import com.google.common.base.Optional;
import edu.iu.dsc.flink.damds.configuration.ConfigurationMgr;
import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
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
  }

  public static void main(String[] args) throws Exception {
    Optional<CommandLine> parserResult =
            parseCommandLineArguments(args, programOptions);
    CommandLine cmd = parserResult.get();
    DAMDSSection config = readConfiguration(cmd);
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DAMDS damds = new DAMDS(config, env);
    damds.setupWorkFlow();
    damds.execute();
  }

  private static DAMDSSection readConfiguration(CommandLine cmd) {
    DAMDSSection config = ConfigurationMgr.LoadConfiguration(
        cmd.getOptionValue(Constants.CMD_OPTION_LONG_C)).damdsSection;
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
