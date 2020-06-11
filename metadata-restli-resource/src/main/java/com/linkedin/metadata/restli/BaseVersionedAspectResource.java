package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.ListResult;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.metadata.validator.ValidationUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CollectionResult;
import com.linkedin.restli.server.CreateKVResponse;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.annotations.ReturnEntity;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.time.Clock;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.BaseReadDAO.*;

/**
 * A base class for an aspect rest.li subresource with versioning support.
 *
 * See http://go/gma for more details
 *
 * @param <URN> must be a valid {@link Urn} type
 * @param <ASPECT_UNION> must be a valid union of aspect models defined in com.linkedin.metadata.aspect
 * @param <ASPECT> must be a valid aspect type inside ASPECT_UNION
 */
public abstract class BaseVersionedAspectResource<URN extends Urn, ASPECT_UNION extends UnionTemplate, ASPECT extends RecordTemplate>
    extends CollectionResourceTaskTemplate<Long, ASPECT> {

  private static final BaseRestliAuditor DUMMY_AUDITOR = new DummyRestliAuditor(Clock.systemUTC());

  private final Class<ASPECT> _aspectClass;

  public BaseVersionedAspectResource(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<ASPECT> aspectClass) {
    super();

    if (!ModelUtils.getValidAspectTypes(aspectUnionClass).contains(aspectClass)) {
      ValidationUtils.invalidSchema("Aspect '%s' is not in Union '%s'", aspectClass.getCanonicalName(),
          aspectUnionClass.getCanonicalName());
    }

    this._aspectClass = aspectClass;
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
   * Constructs an entity-specific {@link Urn} based on the entity's {@link PathKeys}.
   */
  @Nonnull
  protected abstract URN getUrn(@Nonnull PathKeys entityPathKeys);

  @RestMethod.Get
  @Override
  @Nonnull
  public Task<ASPECT> get(@Nonnull Long version) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      return getLocalDAO().get(new AspectKey<>(_aspectClass, urn, version))
          .orElseThrow(RestliUtils::resourceNotFoundException);
    });
  }

  @RestMethod.GetAll
  @Nonnull
  public Task<CollectionResult<ASPECT, ListResultMetadata>> getAllWithMetadata(
      @PagingContextParam @Nonnull PagingContext pagingContext) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());

      final ListResult<ASPECT> listResult =
          getLocalDAO().list(_aspectClass, urn, pagingContext.getStart(), pagingContext.getCount());
      return new CollectionResult<>(listResult.getValues(), listResult.getMetadata());
    });
  }

  @RestMethod.Create
  @Override
  @Nonnull
  public Task<CreateResponse> create(@Nonnull ASPECT aspect) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getLocalDAO().add(urn, aspect, auditStamp);
      return new CreateResponse(HttpStatus.S_201_CREATED);
    });
  }

  /**
   * Similar to {@link #create(RecordTemplate)} but uses a create lambda instead
   */
  @Nonnull
  public Task<CreateResponse> create(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> createLambda) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      getLocalDAO().add(urn, aspectClass, createLambda, auditStamp);
      return new CreateResponse(HttpStatus.S_201_CREATED);
    });
  }

  /**
   * Similar to {@link #create(Class, Function)} but returns {@link CreateKVResponse} containing latest version and created aspect
   */
  @RestMethod.Create
  @ReturnEntity
  @Nonnull
  public Task<CreateKVResponse<Long, ASPECT>> createAndGet(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<Optional<ASPECT>, ASPECT> createLambda) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      final AuditStamp auditStamp = getAuditor().requestAuditStamp(getContext().getRawRequestContext());
      final ASPECT newValue = getLocalDAO().add(urn, aspectClass, createLambda, auditStamp);
      return new CreateKVResponse<>(LATEST_VERSION, newValue);
    });
  }

  /**
   * Creates using the provided default value only if the aspect is not set already
   *
   * @param defaultValue provided default value
   * @return {@link CreateKVResponse} containing lastest version and created aspect
   */
  @RestMethod.Create
  @ReturnEntity
  @Nonnull
  public Task<CreateKVResponse<Long, ASPECT>> createIfAbsent(@Nonnull ASPECT defaultValue) {
    return createAndGet((Class<ASPECT>) defaultValue.getClass(), ignored -> ignored.orElse(defaultValue));
  }
}
