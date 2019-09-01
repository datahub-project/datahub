package com.linkedin.metadata.dao;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.identity.CorpGroupKey;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * A snapshot request builder for corp group info entities.
 */
public class CorpGroupSnapshotRequestBuilder
    extends BaseSnapshotRequestBuilder<CorpGroupSnapshot, CorpGroupUrn> {

  private static final String BASE_URI_TEMPLATE = "corpGroups/{corpGroup}/snapshot";

  public CorpGroupSnapshotRequestBuilder() {
    super(CorpGroupSnapshot.class, CorpGroupUrn.class, BASE_URI_TEMPLATE);
  }

  @Nonnull
  @Override
  protected Map<String, Object> pathKeys(@Nonnull CorpGroupUrn urn) {
    return Collections.singletonMap("corpGroup", new ComplexResourceKey<>(
        new CorpGroupKey().setName(urn.getGroupNameEntity()),
        new EmptyRecord()));
  }
}