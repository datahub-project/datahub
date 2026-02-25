package com.linkedin.metadata.system_info;

/** Constants for system information components */
public class SystemInfoConstants {

  // Component names
  public static final String GMS_COMPONENT_NAME = "GMS";
  public static final String MAE_COMPONENT_NAME = "MAE Consumer";
  public static final String MCE_COMPONENT_NAME = "MCE Consumer";

  // Component keys for remote fetching
  public static final String GMS_COMPONENT_KEY = "gms";
  public static final String MAE_COMPONENT_KEY = "maeConsumer";
  public static final String MCE_COMPONENT_KEY = "mceConsumer";

  private SystemInfoConstants() {
    // Utility class - no instantiation
  }
}
