package com.linkedin.metadata.timeseries;

import com.linkedin.metadata.aspect.EnvelopedAspect;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@AllArgsConstructor
@Data
@Builder
public class TimeseriesScrollResult {
  int numResults;
  int pageSize;
  String scrollId;
  List<EnvelopedAspect> events;
  List<GenericTimeseriesDocument> documents;
}
