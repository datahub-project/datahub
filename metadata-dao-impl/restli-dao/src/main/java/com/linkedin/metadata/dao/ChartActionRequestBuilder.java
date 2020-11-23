package com.linkedin.metadata.dao;

import com.linkedin.common.urn.ChartUrn;
import com.linkedin.metadata.snapshot.ChartSnapshot;


/**
 * An action request builder for chart entities.
 */
public class ChartActionRequestBuilder extends BaseActionRequestBuilder<ChartSnapshot, ChartUrn> {

  private static final String BASE_URI_TEMPLATE = "charts";

  public ChartActionRequestBuilder() {
    super(ChartSnapshot.class, ChartUrn.class, BASE_URI_TEMPLATE);
  }
}