package edu.iu.dsc.flink.damds;

import edu.iu.dsc.flink.damds.configuration.ConfigurationMgr;
import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import org.apache.commons.cli.CommandLine;

public class DAMDSProgram {
  public static DAMDSSection config;
  public static int BlockSize;

  public static void main(String[] args) {

  }

  private static void readConfiguration(CommandLine cmd) {
    config = ConfigurationMgr.LoadConfiguration(
        cmd.getOptionValue(Constants.CMD_OPTION_LONG_C)).damdsSection;
    BlockSize = config.blockSize;
  }
}
