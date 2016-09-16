package edu.iu.dsc.flink.damds;

public class Constants {
  public static final String TARGET_DIMENSION = "targetDimension";
  public static final String TMIN_FACTOR = "tMinFactor";
  public static final String CG_THRESHOLD = "tMinFactor";
  public static final String GLOBAL_COLS = "globalCols";
  public static final String GLOBAL_ROWS = "globalRows";
  public static final String ALPHA = "alpha";
  public static final String THRESHOLD = "threshold";

  static final String PROGRAM_NAME = "DAMDS";

  static final char CMD_OPTION_SHORT_C = 'c';
  static final String CMD_OPTION_LONG_C = "configFile";
  static final String CMD_OPTION_DESCRIPTION_C = "Configuration file";

  public static String errWrongNumOfBytesSkipped(
      int requestedBytesToSkip, int numSkippedBytes) {
    String
        msg =
        "Requested %1$d bytes to skip, but could skip only %2$d bytes";
    return String.format(msg, requestedBytesToSkip, numSkippedBytes);
  }
}
