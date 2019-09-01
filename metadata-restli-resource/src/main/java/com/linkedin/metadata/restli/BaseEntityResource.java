package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.ComplexKeyResourceTaskTemplate;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * A base class for the entity rest.li resource, that supports CRUD methods.
 *
 * See http://go/gma for more details
 *
 * @param <KEY> the resource's key type
 * @param <VALUE> the resource's value type
 * @param <URN> must be a valid {@link Urn} type for the snapshot
 * @param <SNAPSHOT> must be a valid snapshot type defined in com.linkedin.metadata.snapshot
 * @param <ASPECT_UNION> must be a valid aspect union type supported by the snapshot
 */
public abstract class BaseEntityResource<
    // @formatter:off
    KEY extends RecordTemplate,
    VALUE extends RecordTemplate,
    URN extends Urn,
    SNAPSHOT extends RecordTemplate,
    ASPECT_UNION extends UnionTemplate>
    // @formatter:on
    extends ComplexKeyResourceTaskTemplate<KEY, EmptyRecord, VALUE> {

  private static final String[] ALL_ASPECTS = new String[0];

  private final Class<SNAPSHOT> _snapshotClass;
  private final Class<ASPECT_UNION> _aspectUnionClass;
  private final Set<Class<? extends RecordTemplate>> _supportedAspectClasses;
  private final BaseRestliAuditor _auditor;

  public BaseEntityResource(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull BaseRestliAuditor auditor) {
    super();
    ModelUtils.validateSnapshotAspect(snapshotClass, aspectUnionClass);
    _snapshotClass = snapshotClass;
    _aspectUnionClass = aspectUnionClass;
    _supportedAspectClasses = ModelUtils.getValidAspectTypes(_aspectUnionClass);
    _auditor = auditor;
  }

  public BaseEntityResource(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    this(snapshotClass, aspectUnionClass, new DummyRestliAuditor(Clock.systemUTC()));
  }

  /**
   * Returns an aspect-specific {@link BaseLocalDAO}.
   */
  @Nonnull
  protected abstract BaseLocalDAO<ASPECT_UNION, URN> getLocalDAO();

  /**
   * Converts a resource key to URN.
   */
  @Nonnull
  protected abstract URN toUrn(@Nonnull KEY key);

  /**
   * Converts a URN to resource's key.
   */
  @Nonnull
  protected abstract KEY toKey(@Nonnull URN urn);

  /**
   * Converts a snapshot to resource's value.
   */
  @Nonnull
  protected abstract VALUE toValue(@Nonnull SNAPSHOT snapshot);

  /**
   * Converts a resource's value to a snapshot.
   */
  @Nonnull
  protected abstract SNAPSHOT toSnapshot(@Nonnull VALUE value, @Nonnull URN urn);

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @RestMethod.Get
  @Nonnull
  public Task<VALUE> get(@Nonnull ComplexResourceKey<KEY, EmptyRecord> id,
      @QueryParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames) {
    return RestliUtils.toTask(() -> {
      final URN urn = toUrn(id.getKey());
      return getInternal(Collections.singleton(urn), aspectClasses(aspectNames)).get(urn);
    });
  }

  /**
   * Similar to {@link #get(ComplexResourceKey, String[])} but for multiple entities.
   */
  @RestMethod.BatchGet
  @Nonnull
  public Task<Map<ComplexResourceKey<KEY, EmptyRecord>, VALUE>> batchGet(
      @Nonnull Set<ComplexResourceKey<KEY, EmptyRecord>> ids,
      @QueryParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames) {
    return RestliUtils.toTask(() -> {
      final Map<ComplexResourceKey<KEY, EmptyRecord>, URN> urnMap =
          ids.stream().collect(Collectors.toMap(Function.identity(), id -> toUrn(id.getKey())));
      return getInternal(urnMap.values(), aspectClasses(aspectNames)).entrySet()
          .stream()
          .collect(
              Collectors.toMap(e -> new ComplexResourceKey<>(toKey(e.getKey()), new EmptyRecord()), e -> e.getValue()));
    });
  }

  /**
   * Updates the value of an entity where any non-null aspects in the given value will be updated.
   */
  @RestMethod.Update
  @Override
  @Nonnull
  public Task<UpdateResponse> update(@Nonnull ComplexResourceKey<KEY, EmptyRecord> id, @Nonnull VALUE value) {
    return RestliUtils.toTask(() -> {
      final URN urn = toUrn(id.getKey());
      final AuditStamp auditStamp = _auditor.requestAuditStamp(getContext().getRawRequestContext());
      ModelUtils.getAspectsFromSnapshot(toSnapshot(value, urn))
          .forEach(metadata -> getLocalDAO().add(urn, metadata, auditStamp));
      return new UpdateResponse(HttpStatus.S_202_ACCEPTED);
    });
  }

  @Nonnull
  protected Set<Class<? extends RecordTemplate>> aspectClasses(@Nonnull String[] aspectNames) {
    if (aspectNames.length == 0) {
      return _supportedAspectClasses;
    }
    return Arrays.asList(aspectNames).stream().map(ModelUtils::getAspectClass).collect(Collectors.toSet());
  }

  @Nonnull
  protected Map<URN, VALUE> getInternal(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    // Construct the keys to retrieve latest version of all supported aspects for all URNs.
    final Set<AspectKey<URN, ? extends RecordTemplate>> keys = urns.stream()
        .map(urn -> aspectClasses.stream()
            .map(clazz -> new AspectKey<>(clazz, urn, BaseLocalDAO.LATEST_VERSION))
            .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    final Map<URN, List<UnionTemplate>> urnAspectsMap =
        urns.stream().collect(Collectors.toMap(Function.identity(), urn -> new ArrayList<>()));

    getLocalDAO().get(keys)
        .forEach((key, aspect) -> aspect.ifPresent(
            metadata -> urnAspectsMap.get(key.getUrn()).add(ModelUtils.newAspectUnion(_aspectUnionClass, metadata))));

    return urnAspectsMap.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> toValue(newSnapshot(e.getKey(), e.getValue()))));
  }

  @Nonnull
  private SNAPSHOT newSnapshot(@Nonnull URN urn, @Nonnull List<UnionTemplate> aspects) {
    return ModelUtils.newSnapshot(_snapshotClass, urn, aspects);
  }
}
