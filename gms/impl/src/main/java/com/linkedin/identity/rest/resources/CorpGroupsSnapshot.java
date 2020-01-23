package com.linkedin.identity.rest.resources;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.identity.CorpGroupKey;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.restli.BaseSnapshotResource;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
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
 * Rest.li entry point: /corpGroups/{corpGroupKey}/snapshot
 *
 * @deprecated Use corresponding action methods in /corpGroups
 */
@Slf4j
@RestLiCollection(name = "snapshot", namespace = "com.linkedin.identity.corpgroup", parent = CorpGroups.class)
@CreateOnly({"urn"})
public class CorpGroupsSnapshot extends BaseSnapshotResource<CorpGroupUrn, CorpGroupSnapshot, CorpGroupAspect> {

  private static final String CORPGROUP_KEY = CorpGroups.class.getAnnotation(RestLiCollection.class).keyName();

  public CorpGroupsSnapshot() {
    super(CorpGroupSnapshot.class, CorpGroupAspect.class);
  }

  @Inject
  @Named("corpGroupDao")
  private EbeanLocalDAO localDAO;

  @Override
  @RestMethod.Create
  public Task<CreateResponse> create(@Nonnull CorpGroupSnapshot snapshot) {
    return super.create(snapshot);
  }

  @Override
  @RestMethod.Get
  public Task<CorpGroupSnapshot> get(@Nonnull ComplexResourceKey<SnapshotKey, EmptyRecord> snapshotKey) {
    return super.get(snapshotKey);
  }

  @Nonnull
  @Override
  protected BaseLocalDAO<CorpGroupAspect, CorpGroupUrn> getLocalDAO() {
    return localDAO;
  }

  @Nonnull
  @Override
  protected CorpGroupUrn getUrn(@PathKeysParam @Nonnull PathKeys keys) {
    return new CorpGroupUrn(keys.<ComplexResourceKey<CorpGroupKey, EmptyRecord>>get(CORPGROUP_KEY).getKey().getName());
  }
}
