package com.linkedin.metadata.kafka.hook.notification;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;


public class NotificationUtils {

  /**
   * Given an Entity Urn, generates an relative path from it (for rendering in the UI)
   */
  public static String generateEntityPath(final Urn entityUrn) {
    switch (entityUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        return String.format("/dataset/%s", entityUrn.toString());
      case Constants.CHART_ENTITY_NAME:
        return String.format("/chart/%s", entityUrn.toString());
      case Constants.DASHBOARD_ENTITY_NAME:
        return String.format("/dashboard/%s", entityUrn.toString());
      case Constants.DATA_FLOW_ENTITY_NAME:
        return String.format("/pipeline/%s", entityUrn.toString());
      case Constants.DATA_JOB_ENTITY_NAME:
        return String.format("/task/%s", entityUrn.toString());
      case Constants.TAG_ENTITY_NAME:
        return String.format("/tag/%s", entityUrn.toString());
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return String.format("/glossaryTerm/%s", entityUrn.toString());
      case Constants.DOMAIN_ENTITY_NAME:
        return String.format("/domain/%s", entityUrn.toString());
      case Constants.CONTAINER_ENTITY_NAME:
        return String.format("/container/%s", entityUrn.toString());
      case Constants.CORP_USER_ENTITY_NAME:
        return String.format("/user/%s", entityUrn.toString());
      case Constants.CORP_GROUP_ENTITY_NAME:
        return String.format("/group/%s", entityUrn.toString());
      default:
        return "";
    }
  }

  /**
   * Given an Entity Urn, get the type of the entity
   */
  public static String getEntityType(final Urn entityUrn) {
    switch (entityUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        return "Dataset";
      case Constants.CHART_ENTITY_NAME:
        return "Chart";
      case Constants.DASHBOARD_ENTITY_NAME:
        return "Dashboard";
      case Constants.DATA_FLOW_ENTITY_NAME:
        return "Data Pipeline (Flow)";
      case Constants.DATA_JOB_ENTITY_NAME:
        return "Data Task (Job)";
      case Constants.TAG_ENTITY_NAME:
        return "Tag";
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return "Glossary Term";
      case Constants.DOMAIN_ENTITY_NAME:
        return "Domain";
      case Constants.CONTAINER_ENTITY_NAME:
        return "Container";
      case Constants.CORP_USER_ENTITY_NAME:
        return "User";
      case Constants.CORP_GROUP_ENTITY_NAME:
        return "Group";
      case Constants.INCIDENT_ENTITY_NAME:
        return "Incident";
      default:
        return "";
    }
  }

  private NotificationUtils() { }

}
