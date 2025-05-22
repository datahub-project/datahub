package io.openlineage.spark.agent.vendor.delta;

/** Constants used by the Delta Lake vendor integration. */
public class Constants {
  private Constants() {}

  public static final String MERGE_INTO_COMMAND_CLASS =
      "org.apache.spark.sql.delta.commands.MergeIntoCommand";
}
