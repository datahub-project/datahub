package com.linkedin.metadata.restli;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * A base resource class for serving a versioned single-aspect entity values as sub resource. This resource class is
 * meant to be used as a child resource for classes extending from {@link BaseSingleAspectEntitySimpleKeyResource}.
 *
 * The key for the sub-resource is typically a version field which is a Long value. The versioned resources
 * are retrieved using {@link #get(Long)} and {@link #getAllWithMetadata(PagingContext)}.
 *
 * @param <VALUE> the resource's value type
 * @param <URN> must be a valid {@link Urn} type
 * @param <ASPECT_UNION> must be a valid union of aspect models defined in com.linkedin.metadata.aspect
 * @param <ASPECT> must be a valid aspect type inside ASPECT_UNION
 * */
public abstract class BaseSingleAspectEntitySimpleKeyVersionedSubResource<
    // @formatter:off
    VALUE extends RecordTemplate,
    URN extends Urn,
    ASPECT extends RecordTemplate,
    ASPECT_UNION extends UnionTemplate>
    extends CollectionResourceTaskTemplate<Long, VALUE> {
   // @formatter:on

  private final Class<ASPECT> _aspectClass;
  private final Class<VALUE> _valueClass;

  public BaseSingleAspectEntitySimpleKeyVersionedSubResource(
      @Nonnull Class<ASPECT> aspectClass,
      @Nonnull Class<VALUE> valueClass) {

    super();
    this._aspectClass = aspectClass;
    this._valueClass = valueClass;
  }

  /**
   * Returns an aspect-specific {@link BaseLocalDAO}.
   */
  @Nonnull
  protected abstract BaseLocalDAO<ASPECT_UNION, URN> getLocalDAO();

  /**
   * Extracts the parent resource urn from the request keys.
   *
   * */
  @Nonnull
  protected abstract URN getUrn(@Nonnull PathKeys entityPathKeys);

  /**
   * Takes a partial entity created by {@link #createPartialEntityFromAspect(ASPECT)} and the urn and
   * creates the complete entity value.
   *
   * @param partialEntity the partial entity.
   * @param urn urn of the entity.
   * @return the complete entity.
   * */
  @Nonnull
  protected abstract VALUE createEntity(@Nonnull VALUE partialEntity, @Nonnull URN urn);

  /**
   * Gets the versioned resource corresponding to the given input version.
   *
   * @param version version of the aspect.
   * */
  @RestMethod.Get
  @Nonnull
  public Task<VALUE> get(@Nonnull Long version) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      final ASPECT aspect = getLocalDAO().get(new AspectKey<>(_aspectClass, urn, version))
          .orElseThrow(() -> RestliUtils.resourceNotFoundException(
              String.format("Versioned resource for urn %s with the version %s version cannot be found", urn, version)));
      return createEntity(createPartialEntityFromAspect(aspect), urn);
    });
  }

  /**
   * Gets all the versions of a resource given a id and paging context.
   *
   * @param pagingContext Paging context.
   * @return collection of versioned resource(s).
   * */
  @RestMethod.GetAll
  @Nonnull
  public Task<CollectionResult<VALUE, ListResultMetadata>> getAllWithMetadata(
      @PagingContextParam @Nonnull PagingContext pagingContext) {

    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());

      final ListResult<ASPECT> aspects =
          getLocalDAO().list(_aspectClass, urn, pagingContext.getStart(), pagingContext.getCount());

      final List<VALUE> entities = aspects.getValues()
          .stream()
          .map(aspect -> createEntity(createPartialEntityFromAspect(aspect), urn))
          .collect(Collectors.toList());

      return new CollectionResult<>(entities, aspects.getMetadata());
    });
  }

  /**
   * Creates a partial entity value from the aspect. The other fields in the value are set using
   * the {@link #createEntity(ASPECT, URN)} method.
   * */
  private VALUE createPartialEntityFromAspect(@Nonnull ASPECT aspect) {
    try {
      final DataMap data = aspect.data().clone();
      return RecordUtils.toRecordTemplate(_valueClass, data);
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
