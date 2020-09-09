package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.validator.ValidationUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.RestMethod;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.BaseReadDAO.*;


/**
 * A base rest.li resource class for a single aspect entity where the entity includes the fields of an aspect.
 * In pegasus parlance, the aspect fields are included in the entity using the “include” attribute.
 *
 * @param <KEY> the resource's key type
 * @param <VALUE> the resource's value type
 * @param <URN> must be a valid {@link Urn} type for the snapshot
 * @param <ASPECT> must be a valid aspect of the resource
 * @param <ASPECT_UNION> must be a valid aspect union type
 * @param <SNAPSHOT> must be a valid snapshot type defined in com.linkedin.metadata.snapshot
 */
public abstract class BaseSingleAspectEntitySimpleKeyResource<
    // @formatter:off
    KEY,
    VALUE extends RecordTemplate,
    URN extends Urn,
    ASPECT extends RecordTemplate,
    ASPECT_UNION extends UnionTemplate,
    SNAPSHOT extends RecordTemplate>
    // @formatter:on
    extends BaseEntitySimpleKeyResource<KEY, VALUE, URN, SNAPSHOT, ASPECT_UNION> {

  private final Class<VALUE> _valueClass;
  private final Class<ASPECT> _aspectClass;

  /**
   * Constructor.
   */
  public BaseSingleAspectEntitySimpleKeyResource(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<VALUE> valueClass,
      @Nonnull Class<SNAPSHOT> snapshotClass) {

    super(aspectUnionClass, snapshotClass);
    this._aspectClass = aspectClass;
    this._valueClass = valueClass;
  }

  /**
   * Takes a partial entity created by {@link #createPartialEntityFromAspect(ASPECT)} and the urn and
   * creates the complete entity value.
   *
   * @param partialEntity the partial entity.
   * @param urn urn of the entity.
   * @return the complete entity.
   */
  @Nonnull
  protected abstract VALUE createEntity(@Nonnull VALUE partialEntity, @Nonnull URN urn);

  /**
   * Override {@link BaseEntitySimpleKeyResource}'s method to override the default logic of returning entity values
   * for each urn. The base classes assumes that the aspects are fields in the entity value whereas in this class
   * the aspect is included in the value.
   */
  @Override
  @Nonnull
  protected Map<URN, VALUE> getUrnEntityMap(@Nonnull Collection<URN> urns,
      @Nonnull Set<Class<? extends RecordTemplate>> aspectClasses) {
    return getUrnEntityMapInternal(urns);
  }

  @Nonnull
  private Map<URN, VALUE> getUrnEntityMapInternal(@Nonnull Collection<URN> urns) {
    final Set<AspectKey<URN, ? extends RecordTemplate>> aspectKeys =
        urns.stream().map(urn -> new AspectKey<>(_aspectClass, urn, LATEST_VERSION)).collect(Collectors.toSet());

    final Map<AspectKey<URN, ? extends RecordTemplate>, Optional<? extends RecordTemplate>> aspectKeyOptionalAspects =
        getLocalDAO().get(aspectKeys);

    return aspectKeyOptionalAspects.entrySet()
        .stream()
        .filter(entry -> entry.getValue().isPresent())
        .collect(Collectors.toMap(entry -> entry.getKey().getUrn(), entry -> {
          final URN urn = entry.getKey().getUrn();
          @SuppressWarnings("unchecked")
          ASPECT aspect = (ASPECT) entry.getValue().get();
          return createEntity(createPartialEntityFromAspect(aspect), urn);
        }));
  }

  /**
   * Creates a partial entity value from the aspect. The other fields in the value are set using
   * the {@link #createEntity(ASPECT, URN)} method.
   */
  @Nonnull
  private VALUE createPartialEntityFromAspect(@Nonnull ASPECT aspect) {
    try {
      // The fields of the aspect are included in the entity value.
      // Hence, the data map of the aspect can be used to set the data map for the entity value.
      final DataMap aspectDataMap = aspect.data().clone();
      return RecordUtils.toRecordTemplate(_valueClass, aspectDataMap);
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Throwing an exception with a `not implemented` error message as this method is only required
   * by parent class {@link BaseEntitySimpleKeyResource} method- {@link #getUrnEntityMap(Collection, Set)},
   * which has been overridden here.
   */
  @Override
  @Nonnull
  protected VALUE toValue(@Nonnull SNAPSHOT snapshot) {
    throw new RuntimeException("Not implemented.");
  }

  /**
   * Throwing an exception with a `not implemented` error message as this method is only required
   * by parent class {@link BaseEntitySimpleKeyResource} method- {@link #getUrnEntityMap(Collection, Set)},
   * which has been overridden here.
   */
  @Override
  @Nonnull
  protected SNAPSHOT toSnapshot(@Nonnull VALUE value, @Nonnull URN urn) {
    throw new RuntimeException("Not implemented.");
  }

  /**
   * Gets all {@link VALUE} objects from DB for an entity with single aspect
   * Warning: this works only if the aspect is not shared with other entities.
   *
   * It paginates over the latest version of a specific aspect for all Urns
   * By default the list is sorted in ascending order of urn
   *
   * @param pagingContext Paging context.
   * @return collection of latest resource(s).
   */
  @RestMethod.GetAll
  @Nonnull
  public Task<CollectionResult<VALUE, ListResultMetadata>> getAllWithMetadata(
      @PagingContextParam @Nonnull PagingContext pagingContext) {

    if (ModelUtils.isCommonAspect(_aspectClass)) {
      ValidationUtils.invalidSchema("Aspect '%s' is a common aspect that could be shared between multiple entities."
              + "Please use BaseSingleAspectSearchableEntitySimpleKeyResource's GetAll method instead",
          _aspectClass.getCanonicalName());
    }

    return RestliUtils.toTask(() -> {

      final ListResult<ASPECT> aspects =
          getLocalDAO().list(_aspectClass, pagingContext.getStart(), pagingContext.getCount());

      final List<VALUE> entities =
          aspects.getValues().stream().map(this::createPartialEntityFromAspect).collect(Collectors.toList());

      return new CollectionResult<>(entities, aspects.getMetadata());
    });
  }
}
