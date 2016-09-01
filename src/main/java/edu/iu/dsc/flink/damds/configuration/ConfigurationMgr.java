package edu.iu.dsc.flink.damds.configuration;

import edu.iu.dsc.flink.damds.Constants;
import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;
import mpi.MPIException;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

public class ConfigurationMgr {
  private String configurationFilePath;
  public DAMDSSection damdsSection;

  public ConfigurationMgr(String configurationFilePath) {
    this.configurationFilePath = configurationFilePath;
    damdsSection = new DAMDSSection(configurationFilePath);
  }

  public static ConfigurationMgr LoadConfiguration(String configurationFilePath){
    // TODO - Fix configuration management
    return new ConfigurationMgr(configurationFilePath);
  }

  public static Configuration getConfiguration(DAMDSSection config) {
    Configuration configuration = new Configuration();
    configuration.setInteger(Constants.GLOBAL_ROWS, config.numberDataPoints);
    configuration.setInteger(Constants.GLOBAL_COLS, config.numberDataPoints);
    configuration.setInteger(Constants.TARGET_DIMENSION, config.targetDimension);
    configuration.setDouble(Constants.ALPHA, config.alpha);
    configuration.setDouble(Constants.THRESHOLD, config.threshold);
    return configuration;
  }
}
