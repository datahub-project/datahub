package com.linkedin.identity.rest.resources;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.identity.CorpGroup;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.CorpGroupKey;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.restli.BaseEntityResource;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;

import static com.linkedin.metadata.restli.RestliConstants.*;


@RestLiCollection(name = "corpGroups", namespace = "com.linkedin.identity", keyName = "corpGroup")
public final class CorpGroups extends BaseEntityResource<
    // @formatter:off
    CorpGroupKey,
    CorpGroup,
    CorpGroupUrn,
    CorpGroupSnapshot,
    CorpGroupAspect> {
    // @formatter:on

  @Inject
  @Named("corpGroupDao")
  private BaseLocalDAO<CorpGroupAspect, CorpGroupUrn> _localDAO;

  public CorpGroups() {
    super(CorpGroupSnapshot.class, CorpGroupAspect.class);
  }

  @Override
  @Nonnull
  protected BaseLocalDAO<CorpGroupAspect, CorpGroupUrn> getLocalDAO() {
    return _localDAO;
  }

  @Override
  @Nonnull
  protected CorpGroupUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return CorpGroupUrn.deserialize(urnString);
  }

  @Override
  @Nonnull
  protected CorpGroupUrn toUrn(@Nonnull CorpGroupKey corpGroupKey) {
    return new CorpGroupUrn(corpGroupKey.getName());
  }

  @Override
  @Nonnull
  protected CorpGroupKey toKey(@Nonnull CorpGroupUrn urn) {
    return new CorpGroupKey().setName(urn.getGroupNameEntity());
  }

  @Override
  @Nonnull
  protected CorpGroup toValue(@Nonnull CorpGroupSnapshot snapshot) {
    final CorpGroup value = new CorpGroup().setName(snapshot.getUrn().getGroupNameEntity());
    ModelUtils.getAspectsFromSnapshot(snapshot).forEach(aspect -> {
      if (aspect instanceof CorpGroupInfo) {
        value.setInfo(CorpGroupInfo.class.cast(aspect));
      }
    });
    return value;
  }

  @Override
  @Nonnull
  protected CorpGroupSnapshot toSnapshot(@Nonnull CorpGroup corpGroup, @Nonnull CorpGroupUrn urn) {
    final List<CorpGroupAspect> aspects = new ArrayList<>();
    if (corpGroup.hasInfo()) {
      aspects.add(ModelUtils.newAspectUnion(CorpGroupAspect.class, corpGroup.getInfo()));
    }
    return ModelUtils.newSnapshot(CorpGroupSnapshot.class, urn, aspects);
  }

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<CorpGroup> get(@Nonnull ComplexResourceKey<CorpGroupKey, EmptyRecord> key,
      @QueryParam(PARAM_ASPECTS) @Optional("[]") String[] aspectNames) {
    return super.get(key, aspectNames);
  }

  @RestMethod.BatchGet
  @Override
  @Nonnull
  public Task<Map<ComplexResourceKey<CorpGroupKey, EmptyRecord>, CorpGroup>> batchGet(
      @Nonnull Set<ComplexResourceKey<CorpGroupKey, EmptyRecord>> keys,
      @QueryParam(PARAM_ASPECTS) @Optional("[]") String[] aspectNames) {
    return super.batchGet(keys, aspectNames);
  }

  @Action(name = ACTION_INGEST)
  @Override
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull CorpGroupSnapshot snapshot) {
    return super.ingest(snapshot);
  }

  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<CorpGroupSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames) {
    return super.getSnapshot(urnString, aspectNames);
  }

  @Action(name = ACTION_BACKFILL)
  @Override
  @Nonnull
  public Task<String[]> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames) {
    return super.backfill(urnString, aspectNames);
  }
}
