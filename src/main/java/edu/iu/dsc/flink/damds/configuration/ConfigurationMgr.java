package edu.iu.dsc.flink.damds.configuration;

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

  public static Configuration getConfiguration(DAMDSSection config)
      throws IOException, MPIException {
    Configuration configuration = new Configuration();
    configuration.setInteger("globalRows", config.numberDataPoints);
    configuration.setInteger("globalCols", config.numberDataPoints);
    configuration.setInteger("targetDimention", config.targetDimension);
    configuration.setDouble("alpha", config.alpha);
    return configuration;
  }
}
