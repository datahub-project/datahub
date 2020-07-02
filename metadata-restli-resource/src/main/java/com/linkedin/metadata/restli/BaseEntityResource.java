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
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
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

import static com.linkedin.metadata.dao.BaseReadDAO.*;
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

  private static final BaseRestliAuditor DUMMY_AUDITOR = new DummyRestliAuditor(Clock.systemUTC());

  private final Class<SNAPSHOT> _snapshotClass;
  private final Class<ASPECT_UNION> _aspectUnionClass;
  private final Set<Class<? extends RecordTemplate>> _supportedAspectClasses;

  public BaseEntityResource(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    super();
    ModelUtils.validateSnapshotAspect(snapshotClass, aspectUnionClass);
    _snapshotClass = snapshotClass;
    _aspectUnionClass = aspectUnionClass;
    _supportedAspectClasses = ModelUtils.getValidAspectTypes(_aspectUnionClass);
  }

  /**
   * Returns a {@link BaseRestliAuditor} for this resource.
   */
  @Nonnull
  protected BaseRestliAuditor getAuditor() {
    return DUMMY_AUDITOR;
  }

  /**
   * Returns an aspect-specific {@link BaseLocalDAO}.
   */
  @Nonnull
  protected abstract BaseLocalDAO<ASPECT_UNION, URN> getLocalDAO();

  /**
   * Creates an URN from its string representation.
   */
  @Nonnull
  protected abstract URN createUrnFromString(@Nonnull String urnString) throws Exception;

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
      final VALUE value = getInternalNonEmpty(Collections.singleton(urn), parseAspectsParam(aspectNames)).get(urn);
      if (value == null) {
        throw RestliUtils.resourceNotFoundException();
      }
      return value;
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
      return getInternal(urnMap.values(), parseAspectsParam(aspectNames)).entrySet()
          .stream()
          .collect(
              Collectors.toMap(e -> new ComplexResourceKey<>(toKey(e.getKey()), new EmptyRecord()), e -> e.getValue()));
    });
  }

  /**
   * An action method for automated ingestion pipeline.
   */
  @Action(name = ACTION_INGEST)
  @Nonnull
  public Task<Void> ingest(@ActionParam(PARAM_SNAPSHOT) @Nonnull SNAPSHOT snapshot) {
    return ingestInternal(snapshot, Collections.emptySet());
  }

  @Nonnull
  protected Task<Void> ingestInternal(@Nonnull SNAPSHOT snapshot,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectsToIgnore) {
    return RestliUtils.toTask(() -> {
      final URN urn = (URN) ModelUtils.getUrnFromSnapshot(snapshot);
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      ModelUtils.getAspectsFromSnapshot(snapshot).stream().forEach(aspect -> {
        if (!aspectsToIgnore.contains(aspect.getClass())) {
          getLocalDAO().add(urn, aspect, auditStamp);
        }
      });
      return null;
    });
  }

  /**
   * An action method for getting a snapshot of aspects for an entity.
   */
  @Action(name = ACTION_GET_SNAPSHOT)
  @Nonnull
  public Task<SNAPSHOT> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames) {

    return RestliUtils.toTask(() -> {
      final URN urn = parseUrnParam(urnString);
      final Set<AspectKey<URN, ? extends RecordTemplate>> keys = parseAspectsParam(aspectNames).stream()
          .map(aspectClass -> new AspectKey<>(aspectClass, urn, LATEST_VERSION))
          .collect(Collectors.toSet());

      final List<UnionTemplate> aspects = getLocalDAO().get(keys)
          .values()
          .stream()
          .filter(java.util.Optional::isPresent)
          .map(aspect -> ModelUtils.newAspectUnion(_aspectUnionClass, aspect.get()))
          .collect(Collectors.toList());

      return ModelUtils.newSnapshot(_snapshotClass, urn, aspects);
    });
  }

  /**
   * An action method for emitting MAE backfill messages for an entity.
   */
  @Action(name = ACTION_BACKFILL)
  @Nonnull
  public Task<String[]> backfill(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional("[]") @Nonnull String[] aspectNames) {

    return RestliUtils.toTask(() -> {
      final URN urn = parseUrnParam(urnString);
      final List<String> backfilledAspects = parseAspectsParam(aspectNames).stream()
          .map(aspectClass -> getLocalDAO().backfill(aspectClass, urn))
          .filter(optionalAspect -> optionalAspect.isPresent())
          .map(optionalAspect -> ModelUtils.getAspectName(optionalAspect.get().getClass()))
          .collect(Collectors.toList());
      return backfilledAspects.toArray(new String[0]);
    });
  }

  @Nonnull
  protected Set<Class<? extends RecordTemplate>> parseAspectsParam(@Nonnull String[] aspectNames) {
    if (aspectNames.length == 0) {
      return _supportedAspectClasses;
    }
    return Arrays.asList(aspectNames).stream().map(ModelUtils::getAspectClass).collect(Collectors.toSet());
  }

  /**
   * Returns a map of {@link VALUE} models given the collection of {@link URN}s and set of aspect classes
   *
   * @param urns collection of urns
   * @param aspectClasses set of aspect classes
   * @return All {@link VALUE} objects keyed by {@link URN} obtained from DB
   */
  @Nonnull
  protected Map<URN, VALUE> getInternal(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    return getUrnAspectMap(urns, aspectClasses).entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> toValue(newSnapshot(e.getKey(), e.getValue()))));
  }

  /**
   * Similar to {@link #getInternal(Collection, Set)} but filter out {@link URN}s which are not in the DB.
   */
  @Nonnull
  protected Map<URN, VALUE> getInternalNonEmpty(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    return getUrnAspectMap(urns, aspectClasses).entrySet()
        .stream()
        .filter(e -> !e.getValue().isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey, e -> toValue(newSnapshot(e.getKey(), e.getValue()))));
  }

  @Nonnull
  private Map<URN, List<UnionTemplate>> getUrnAspectMap(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    // Construct the keys to retrieve latest version of all supported aspects for all URNs.
    final Set<AspectKey<URN, ? extends RecordTemplate>> keys = urns.stream()
        .map(urn -> aspectClasses.stream()
            .map(clazz -> new AspectKey<>(clazz, urn, LATEST_VERSION))
            .collect(Collectors.toList()))
        .flatMap(List::stream)
        .collect(Collectors.toSet());

    final Map<URN, List<UnionTemplate>> urnAspectsMap =
        urns.stream().collect(Collectors.toMap(Function.identity(), urn -> new ArrayList<>()));

    getLocalDAO().get(keys)
        .forEach((key, aspect) -> aspect.ifPresent(
            metadata -> urnAspectsMap.get(key.getUrn()).add(ModelUtils.newAspectUnion(_aspectUnionClass, metadata))));

    return urnAspectsMap;
  }

  @Nonnull
  private SNAPSHOT newSnapshot(@Nonnull URN urn, @Nonnull List<UnionTemplate> aspects) {
    return ModelUtils.newSnapshot(_snapshotClass, urn, aspects);
  }

  @Nonnull
  private URN parseUrnParam(@Nonnull String urnString) {
    try {
      return createUrnFromString(urnString);
    } catch (Exception e) {
      throw RestliUtils.badRequestException("Invalid URN: " + urnString);
    }
  }
}
