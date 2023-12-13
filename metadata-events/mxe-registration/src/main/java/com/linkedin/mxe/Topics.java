package com.linkedin.mxe;

public class Topics {
  public static final String METADATA_AUDIT_EVENT = "MetadataAuditEvent_v4";
  public static final String METADATA_CHANGE_EVENT = "MetadataChangeEvent_v4";
  public static final String FAILED_METADATA_CHANGE_EVENT = "FailedMetadataChangeEvent_v4";
  public static final String DATAHUB_USAGE_EVENT = "DataHubUsageEvent_v1";
  public static final String METADATA_GRAPH_EVENT = "MetadataGraphEvent_v4";
  public static final String METADATA_SEARCH_EVENT = "MetadataSearchEvent_v4";

  public static final String METADATA_CHANGE_LOG_VERSIONED = "MetadataChangeLog_Versioned_v1";
  public static final String METADATA_CHANGE_LOG_TIMESERIES = "MetadataChangeLog_Timeseries_v1";
  public static final String METADATA_CHANGE_PROPOSAL = "MetadataChangeProposal_v1";
  public static final String FAILED_METADATA_CHANGE_PROPOSAL = "FailedMetadataChangeProposal_v1";
  public static final String PLATFORM_EVENT = "PlatformEvent_v1";
  public static final String DATAHUB_UPGRADE_HISTORY_TOPIC_NAME = "DataHubUpgradeHistory_v1";

  public static final String DEV_METADATA_AUDIT_EVENT = "MetadataAuditEvent_v4_dev";
  public static final String DEV_METADATA_CHANGE_EVENT = "MetadataChangeEvent_v4_dev";
  public static final String DEV_FAILED_METADATA_CHANGE_EVENT = "FailedMetadataChangeEvent_v4_dev";

  /** aspect-specific MAE topics. format : METADATA_AUDIT_EVENT_<URN>_<ASPECT> */
  // MAE topics for CorpGroup entity.
  public static final String METADATA_AUDIT_EVENT_CORPGROUP_CORPGROUPINFO =
      "MetadataAuditEvent_CorpGroup_CorpGroupInfo_v1";

  // MAE topics for CorpUser entity.
  public static final String METADATA_AUDIT_EVENT_CORPUSER_CORPUSEREDITABLEINFO =
      "MetadataAuditEvent_CorpUser_CorpUserEditableInfo_v2";
  public static final String METADATA_AUDIT_EVENT_CORPUSER_CORPUSERINFO =
      "MetadataAuditEvent_CorpUser_CorpUserInfo_v2";

  /** aspect-specific MCE topics. format : METADATA_CHANGE_EVENT_<URN>_<ASPECT> */
  // MCE topics for CorpGroup entity.
  public static final String METADATA_CHANGE_EVENT_CORPGROUP_CORPGROUPINFO =
      "MetadataChangeEvent_CorpGroup_CorpGroupInfo_v1";

  // MCE topics for CorpUser entity.
  public static final String METADATA_CHANGE_EVENT_CORPUSER_CORPUSEREDITABLEINFO =
      "MetadataChangeEvent_CorpUser_CorpUserEditableInfo_v1";
  public static final String METADATA_CHANGE_EVENT_CORPUSER_CORPUSERINFO =
      "MetadataChangeEvent_CorpUser_CorpUserInfo_v1";

  /** aspect-specific FMCE topics. format : FAILED_METADATA_CHANGE_EVENT_<URN>_<ASPECT> */
  // FMCE topics for CorpGroup entity.
  public static final String FAILED_METADATA_CHANGE_EVENT_CORPGROUP_CORPGROUPINFO =
      "FailedMetadataChangeEvent_CorpGroup_CorpGroupInfo_v1";

  // FMCE topics for CorpUser entity.
  public static final String FAILED_METADATA_CHANGE_EVENT_CORPUSER_CORPUSEREDITABLEINFO =
      "FailedMetadataChangeEvent_CorpUser_CorpUserEditableInfo_v1";
  public static final String FAILED_METADATA_CHANGE_EVENT_CORPUSER_CORPUSERINFO =
      "FailedMetadataChangeEvent_CorpUser_CorpUserInfo_v1";

  private Topics() {
    // Util class
  }
}
