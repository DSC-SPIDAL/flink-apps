package edu.iu.dsc.flink.damds.configuration;

import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;

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
}
