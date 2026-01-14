package com.linkedin.metadata.aspect;

import com.datahub.authorization.AuthorizationSession;
import com.linkedin.metadata.entity.SearchRetriever;
import javax.annotation.Nullable;

public interface RetrieverContext {
  GraphRetriever getGraphRetriever();

  AspectRetriever getAspectRetriever();

  SearchRetriever getSearchRetriever();

  @Nullable
  AuthorizationSession getAuthorizationSession();
}
