package com.linkedin.metadata.dao;

import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.metadata.snapshot.DashboardSnapshot;


/**
 * An action request builder for dashboard entities.
 */
public class DashboardActionRequestBuilder extends BaseActionRequestBuilder<DashboardSnapshot, DashboardUrn> {

  private static final String BASE_URI_TEMPLATE = "dashboards";

  public DashboardActionRequestBuilder() {
    super(DashboardSnapshot.class, DashboardUrn.class, BASE_URI_TEMPLATE);
  }
}