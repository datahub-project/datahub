package com.linkedin.metadata.aspect.plugins.validation;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.r2.RemoteInvocationException;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;

public interface AspectRetriever {

  Aspect getLatestAspectObject(@Nonnull final Urn urn, @Nonnull final String aspectName)
      throws RemoteInvocationException, URISyntaxException;
}
