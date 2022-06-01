package com.linkedin.metadata.kafka.hook.notification;


/**
 * A standard set of event types which trigger notifications. Formally supported by DataHub.
 *
 * Note that scenario types differ from {@link com.datahub.notification.NotificationTemplateType}s
 * in that templates simply represent the format / shape / template of a particular message,
 * whereas scenario types denote high-level event types that occur on the DataHub platform and may
 * or may not trigger notifications to various stakeholders: global channels, owners, downstream owners,
 * and more.
 *
 * When users of DataHub are changing their settings, they are most often changing settings that are keyed
 * by notification scenario types.
 */
public enum NotificationScenarioType {
  ENTITY_TAG_CHANGE,
  ENTITY_GLOSSARY_TERM_CHANGE,
  ENTITY_OWNER_CHANGE,
  ENTITY_DOMAIN_CHANGE,
  ENTITY_DEPRECATION_CHANGE,
  DATASET_SCHEMA_CHANGE,
  NEW_INCIDENT,
  INCIDENT_STATUS_CHANGE,
  NEW_PROPOSAL,
  PROPOSAL_STATUS_CHANGE,
  INGESTION_RUN_CHANGE,
}
