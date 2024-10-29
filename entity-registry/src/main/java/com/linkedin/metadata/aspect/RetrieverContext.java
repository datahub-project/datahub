package com.linkedin.metadata.aspect;

import com.linkedin.metadata.entity.SearchRetriever;

public interface RetrieverContext {
  GraphRetriever getGraphRetriever();

  AspectRetriever getAspectRetriever();

  SearchRetriever getSearchRetriever();
}
