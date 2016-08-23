package edu.iu.dsc.flink.damds;

import edu.iu.dsc.flink.damds.configuration.section.DAMDSSection;

public class DAMDS {
  public DAMDSSection config;

  public DAMDS(DAMDSSection config) {
    this.config = config;
  }

  public void execute() {
    // execute BC

    // now execute CG
  }
}
