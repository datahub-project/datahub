resource "kafka_topic" "MetadataAuditEvent_v4" {
  name               = "MetadataAuditEvent_v4"
  replication_factor = 3
  partitions         = 1
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "2592000000" # 30 days
  }
}

resource "kafka_topic" "MetadataChangeEvent_v4" {
  name               = "MetadataChangeEvent_v4"
  replication_factor = 3
  partitions         = 1
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "2592000000" # 30 days
  }
}

resource "kafka_topic" "FailedMetadataChangeEvent_v4" {
  name               = "FailedMetadataChangeEvent_v4"
  replication_factor = 3
  partitions         = 1
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "2592000000" # 30 days
  }
}

resource "kafka_topic" "DataHubUsageEvent_v1" {
  name               = "DataHubUsageEvent_v1"
  replication_factor = 3
  partitions         = 1
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "2592000000" # 30 days
  }
}

resource "kafka_topic" "MetadataChangeLog_Versioned_v1" {
  name               = "MetadataChangeLog_Versioned_v1"
  replication_factor = 3
  partitions         = 1
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "2592000000" # 30 days
  }
}

resource "kafka_topic" "MetadataChangeLog_Timeseries_v1" {
  name               = "MetadataChangeLog_Timeseries_v1"
  replication_factor = 3
  partitions         = 1
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "2592000000" # 30 days
  }
}

resource "kafka_topic" "MetadataChangeProposal_v1" {
  name               = "MetadataChangeProposal_v1"
  replication_factor = 3
  partitions         = 1
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "2592000000" # 30 days
  }
}

resource "kafka_topic" "FailedMetadataChangeProposal_v1" {
  name               = "FailedMetadataChangeProposal_v1"
  replication_factor = 3
  partitions         = 1
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = "2592000000" # 30 days
  }
}
