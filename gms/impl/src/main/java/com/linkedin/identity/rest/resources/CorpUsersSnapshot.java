package com.linkedin.identity.rest.resources;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.identity.CorpUserKey;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.restli.BaseSnapshotResource;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import com.linkedin.metadata.snapshot.SnapshotKey;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.validation.CreateOnly;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.annotations.PathKeysParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;


/**
 * Rest.li entry point: /corpUsers/{corpUserKey}/snapshot
 *
 * @deprecated Use corresponding action methods in /corpUsers
 */
@Slf4j
@RestLiCollection(name = "snapshot", namespace = "com.linkedin.identity.corpuser", parent = CorpUsers.class)
@CreateOnly({"urn"})
public class CorpUsersSnapshot extends BaseSnapshotResource<CorpuserUrn, CorpUserSnapshot, CorpUserAspect> {

  private static final String CORPUSER_KEY = CorpUsers.class.getAnnotation(RestLiCollection.class).keyName();

  public CorpUsersSnapshot() {
    super(CorpUserSnapshot.class, CorpUserAspect.class);
  }

  @Inject
  @Named("corpUserDao")
  private EbeanLocalDAO localDAO;

  @Override
  @RestMethod.Create
  public Task<CreateResponse> create(@Nonnull CorpUserSnapshot corpUserSnapshot) {
    return super.create(corpUserSnapshot);
  }

  @Override
  @RestMethod.Get
  public Task<CorpUserSnapshot> get(@Nonnull ComplexResourceKey<SnapshotKey, EmptyRecord> snapshotKey) {
    return super.get(snapshotKey);
  }

  @Nonnull
  @Override
  protected BaseLocalDAO<CorpUserAspect, CorpuserUrn> getLocalDAO() {
    return localDAO;
  }

  @Nonnull
  @Override
  protected CorpuserUrn getUrn(@PathKeysParam @Nonnull PathKeys keys) {
    return new CorpuserUrn(keys.<ComplexResourceKey<CorpUserKey, EmptyRecord>>get(CORPUSER_KEY).getKey().getName());
  }
}
