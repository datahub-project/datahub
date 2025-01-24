package com.linkedin.mxe;

public class ConsumerGroups {
  private ConsumerGroups() {}

  public static final String MCP_CONSUMER_GROUP_ID_VALUE =
      "${METADATA_CHANGE_PROPOSAL_KAFKA_CONSUMER_GROUP_ID:generic-mce-consumer-job-client}";
}
