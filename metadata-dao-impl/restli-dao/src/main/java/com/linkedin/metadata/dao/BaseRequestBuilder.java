package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.restli.client.Request;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;


public abstract class BaseRequestBuilder<SNAPSHOT extends RecordTemplate, URN extends Urn> {

  /**
   * Gets the specific {@link Urn} type supported by this builder.
   */
  @Nonnull
  public abstract Class<URN> urnClass();

  /**
   * Returns a rest.li {@link Request} to retrieve a specific version of a metadata aspect for an entity.
   *
   * @deprecated Retrieval of versioned aspect is no longer supported. Use {@link #getRequest(Set, Urn)} instead.
   */
  @Nonnull
  public abstract Request<SNAPSHOT> getRequest(@Nonnull String aspectName, @Nonnull URN urn, long version);

  /**
   * Returns a rest.li {@link Request} to retrieve a set of metadata aspects for an entity.
   */
  @Nonnull
  public abstract Request<SNAPSHOT> getRequest(@Nonnull Set<AspectVersion> aspectVersions, @Nonnull URN urn);

  /**
   * Returns a rest.li {@link Request} to create a snapshot for an entity.
   */
  @Nonnull
  public abstract Request createRequest(@Nonnull URN urn, @Nonnull SNAPSHOT snapshot);

  @Nonnull
  protected Map<String, Object> pathKeys(@Nonnull URN urn) {
    return Collections.EMPTY_MAP;
  }

}
