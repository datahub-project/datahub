package com.linkedin.metadata.dao;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpUserKey;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;


/**
 * A snapshot request builder for corp user info entities.
 */
public class CorpUserSnapshotRequestBuilder
    extends BaseSnapshotRequestBuilder<CorpUserSnapshot, CorpuserUrn> {

  private static final String BASE_URI_TEMPLATE = "corpUsers/{corpUser}/snapshot";

  public CorpUserSnapshotRequestBuilder() {
    super(CorpUserSnapshot.class, CorpuserUrn.class, BASE_URI_TEMPLATE);
  }

  @Nonnull
  @Override
  protected Map<String, Object> pathKeys(@Nonnull CorpuserUrn urn) {
    return Collections.singletonMap("corpUser", new ComplexResourceKey<>(
        new CorpUserKey().setName(urn.getUsernameEntity()),
        new EmptyRecord()));
  }
}