package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.validator.ValidationUtils;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.resources.BaseResource;
import com.linkedin.restli.server.resources.SimpleResourceTaskTemplate;
import java.time.Clock;
import java.util.function.Function;
import javax.annotation.Nonnull;


/**
 * A base class for the aspect rest.li resource
 *
 * See http://go/gma for more details
 *
 * @param <URN> must be a valid {@link Urn} type
 * @param <ASPECT_UNION> must be a valid union of aspect models defined in com.linkedin.metadata.aspect
 * @param <ASPECT> must be a valid aspect type inside ASPECT_UNION
 *
 * @deprecated Use {@link BaseVersionedAspectResource} instead.
 */
public abstract class BaseAspectResource<
    // @formatter:off
    URN extends Urn,
    ASPECT_UNION extends UnionTemplate,
    ASPECT extends RecordTemplate>
    // @formatter:on
    extends SimpleResourceTaskTemplate<ASPECT> implements BaseResource {

  private final Class<ASPECT> _aspectClass;
  private final BaseRestliAuditor _auditor;

  public BaseAspectResource(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<ASPECT> aspectClass) {
    this(aspectUnionClass, aspectClass, new DummyRestliAuditor(Clock.systemUTC()));
  }

  public BaseAspectResource(@Nonnull Class<ASPECT_UNION> aspectUnionClass, @Nonnull Class<ASPECT> aspectClass,
      @Nonnull BaseRestliAuditor auditor) {
    super();

    if (!ModelUtils.getValidAspectTypes(aspectUnionClass).contains(aspectClass)) {
      ValidationUtils.invalidSchema("Aspect '%s' is not in Union '%s'", aspectClass.getCanonicalName(),
          aspectUnionClass.getCanonicalName());
    }

    this._aspectClass = aspectClass;
    this._auditor = auditor;
  }

  /**
   * Returns a aspect-specific {@link BaseLocalDAO}.
   */
  @Nonnull
  protected abstract BaseLocalDAO<ASPECT_UNION, URN> getLocalDAO();

  /**
   * Constructs an entity-specific {@link Urn} based on the entity's {@link PathKeys}.
   */
  @Nonnull
  protected abstract URN getUrn(@Nonnull PathKeys entityPathKeys);

  /**
   * Creates an aspect of metadata for an entity.
   */
  @Override
  @Nonnull
  public Task<UpdateResponse> update(@Nonnull ASPECT aspect) {
    return update((Class<ASPECT>) aspect.getClass(), oldValue -> aspect);
  }

  /**
   * Similar to {@link #update(RecordTemplate)} but uses an update lambda instead
   */
  @Nonnull
  public Task<UpdateResponse> update(@Nonnull Class<ASPECT> aspectClass,
      @Nonnull Function<java.util.Optional<RecordTemplate>, RecordTemplate> updateLambda) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      final AuditStamp auditStamp = _auditor.requestAuditStamp(getContext().getRawRequestContext());
      getLocalDAO().add(urn, aspectClass, updateLambda, auditStamp);
      return new UpdateResponse(HttpStatus.S_201_CREATED);
    });
  }

  /**
   * Retrieves a certain version of metadata aspect for an entity.
   * Actual resource class might have query parameter to default to latest version 0
   */
  @Nonnull
  public Task<ASPECT> get(@QueryParam("version") @Optional("0") long version) {
    return RestliUtils.toTaskFromOptional(() -> {
      if (version < 0) {
        throw new IllegalArgumentException("Invalid version number: " + version);
      }
      final URN urn = getUrn(getContext().getPathKeys());
      return getLocalDAO().get(_aspectClass, urn, version);
    });
  }
}
