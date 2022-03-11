package com.linkedin.metadata.datahubusage;

import lombok.Getter;


@Getter
public enum DataHubUsageEventType {
  PAGE_VIEW_EVENT("PageViewEvent"),
  LOG_IN_EVENT("LogInEvent"),
  LOG_OUT_EVENT("LogOutEvent"),
  SEARCH_EVENT("SearchEvent"),
  SEARCH_RESULTS_VIEW_EVENT("SearchResultsViewEvent"),
  SEARCH_RESULT_CLICK_EVENT("SearchResultClickEvent"),
  BROWSE_RESULT_CLICK_EVENT("BrowseResultClickEvent"),
  ENTITY_VIEW_EVENT("EntityViewEvent"),
  ENTITY_SECTION_VIEW_EVENT("EntitySectionViewEvent"),
  ENTITY_ACTION_EVENT("EntityActionEvent"),
  RECOMMENDATION_IMPRESSION_EVENT("RecommendationImpressionEvent"),
  RECOMMENDATION_CLICK_EVENT("RecommendationClickEvent");

  private final String type;

  DataHubUsageEventType(String type) {
    this.type = type;
  }

  public static DataHubUsageEventType getType(String name) {
    for (DataHubUsageEventType eventType : DataHubUsageEventType.values()) {
      if (eventType.type.equalsIgnoreCase(name)) {
        return eventType;
      }
    }
    return null;
  }
}
