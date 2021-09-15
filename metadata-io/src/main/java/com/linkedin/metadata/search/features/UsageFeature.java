package com.linkedin.metadata.search.features;

import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class UsageFeature implements FeatureExtractor {

  TimeseriesAspectService _timeseriesAspectService;

  @Override
  public List<Features> extractFeatures(List<SearchEntity> entities) {
    return null;
  }
}
