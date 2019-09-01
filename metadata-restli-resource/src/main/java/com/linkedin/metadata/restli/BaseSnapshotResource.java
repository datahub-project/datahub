package com.linkedin.metadata.restli;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.aspect.AspectVersionArray;
import com.linkedin.metadata.dao.AspectKey;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.SnapshotKey;
import com.linkedin.parseq.Task;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.RestMethod;
import com.linkedin.restli.server.resources.BaseResource;
import com.linkedin.restli.server.resources.ComplexKeyResourceTaskTemplate;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * A base class for the snapshot rest.li resource
 *
 * Subclasses should should have a corresponding get, create, and backfill rest.li methods that invoke the versions
 * specified in this class like the example below. This is necessary because the rest.li annotations can't be inherited
 * through class hierarchy.
 *
 * <code>
 * public final class SnapshotResource extends BaseSnapshotResource<FeatureUrn, FeatureSnapshot, FeatureAspect> {
 *
 *   @RestMethod.Create
 *   public Task<CreateResponse> create(FeatureSnapshot featureSnapshot) {
 *     return super.create(featureSnapshot);
 *   }
 *
 *   @RestMethod.Get
 *   public Task<FeatureSnapshot> get(ComplexResourceKey<SnapshotKey, EmptyRecord> snapshotKey) {
 *     return super.get(snapshotKey);
 *   }
 *
 *   @Action(name = BACKFILL_ACTION_NAME)
 *   public Task<FeatureSnapshot> backfill(@ActionParam(ASPECT_NAMES_PARAM_NAME) String[] aspectNames) {
 *     return super.backfill(aspectNames);
 *   }
 * }
 * </code>
 *
 * @param <URN> must be a valid {@link Urn} type
 * @param <SNAPSHOT> must be a valid snapshot model defined in com.linkedin.metadata.snapshot
 * @param <ASPECT_UNION> must be a valid aspect union type supported by the snapshot
 */
public abstract class BaseSnapshotResource<
    // @formatter:off
    URN extends Urn,
    SNAPSHOT extends RecordTemplate,
    ASPECT_UNION extends UnionTemplate>
    // @formatter:on
    extends ComplexKeyResourceTaskTemplate<SnapshotKey, EmptyRecord, SNAPSHOT> implements BaseResource {

  public static final String BACKFILL_ACTION_NAME = "backfill";
  public static final String ASPECT_NAMES_PARAM_NAME = "aspectNames";

  private final Class<SNAPSHOT> _snapshotClass;
  private final Class<ASPECT_UNION> _aspectUnionClass;
  private final Set<String> _supportedMetadataClassNames;
  private final BaseRestliAuditor _auditor;

  public BaseSnapshotResource(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass) {
    this(snapshotClass, aspectUnionClass, new DummyRestliAuditor(Clock.systemUTC()));
  }

  public BaseSnapshotResource(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<ASPECT_UNION> aspectUnionClass,
      @Nonnull BaseRestliAuditor auditor) {
    super();
    ModelUtils.validateSnapshotAspect(snapshotClass, aspectUnionClass);

    _snapshotClass = snapshotClass;
    _aspectUnionClass = aspectUnionClass;
    _auditor = auditor;

    _supportedMetadataClassNames = ModelUtils.getValidAspectTypes(_aspectUnionClass)
        .stream()
        .map(Class::getCanonicalName)
        .collect(Collectors.toSet());
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
   * Creates a snapshot of metadata for an entity.
   */
  @RestMethod.Create
  @Override
  @Nonnull
  public Task<CreateResponse> create(@Nonnull SNAPSHOT snapshot) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());
      final AuditStamp auditStamp = _auditor.requestAuditStamp(getContext().getRawRequestContext());
      final List<AspectVersion> aspectVersions = ModelUtils.getAspectsFromSnapshot(snapshot).stream().map(metadata -> {
        getLocalDAO().add(urn, metadata, auditStamp);
        return ModelUtils.newAspectVersion(metadata.getClass(), BaseLocalDAO.LATEST_VERSION);
      }).collect(Collectors.toList());

      final SnapshotKey snapshotKey = new SnapshotKey();
      snapshotKey.setAspectVersions(new AspectVersionArray(aspectVersions));
      return new CreateResponse(new ComplexResourceKey<>(snapshotKey, new EmptyRecord()), HttpStatus.S_201_CREATED);
    });
  }

  /**
   * Retrieves a snapshot of metadata for an entity.
   */
  @RestMethod.Get
  @Override
  @Nonnull
  public Task<SNAPSHOT> get(@Nonnull ComplexResourceKey<SnapshotKey, EmptyRecord> snapshotKey) {
    return RestliUtils.toTask(() -> {
      final List<AspectVersion> aspectVersions = snapshotKey.getKey().getAspectVersions();
      final URN urn = getUrn(getContext().getPathKeys());
      final List<UnionTemplate> aspects = get(urn, aspectVersions);
      return ModelUtils.newSnapshot(_snapshotClass, urn, aspects);
    });
  }

  /**
   * Performs an MetadataAuditEvent backfill for an entity.
   *
   * @param aspectNames a list of FQCN for aspects for which a backfill MAE should be emitted
   * @return a snapshot that includes the aspects for which a backfill MAE has been emitted
   */
  @Action(name = BACKFILL_ACTION_NAME)
  public Task<SNAPSHOT> backfill(@ActionParam(ASPECT_NAMES_PARAM_NAME) @Nonnull String[] aspectNames) {
    return RestliUtils.toTask(() -> {
      final URN urn = getUrn(getContext().getPathKeys());

      final List<UnionTemplate> aspects = Arrays.asList(aspectNames)
          .stream()
          .map(aspectName -> ModelUtils.getAspectClass(aspectName))
          .map(type -> getLocalDAO().backfill(type, urn))
          .filter(optionalMetadata -> optionalMetadata.isPresent())
          .map(optionalMetadata -> ModelUtils.newAspectUnion(_aspectUnionClass, optionalMetadata.get()))
          .collect(Collectors.toList());
      return ModelUtils.newSnapshot(_snapshotClass, urn, aspects);
    });
  }

  @Nonnull
  private List<UnionTemplate> get(@Nonnull URN urn, @Nonnull List<AspectVersion> aspectVersions) {
    final Set<AspectKey<URN, ? extends RecordTemplate>> keys = aspectVersions.stream()
        .map(aspectVersion -> new AspectKey<>(getMetadataType(aspectVersion), urn, aspectVersion.getVersion()))
        .collect(Collectors.toSet());

    return getLocalDAO().get(keys)
        .values()
        .stream()
        .filter(Optional::isPresent)
        .map(aspect -> ModelUtils.newAspectUnion(_aspectUnionClass, aspect.get()))
        .collect(Collectors.toList());
  }

  @Nonnull
  private Class<? extends RecordTemplate> getMetadataType(@Nonnull AspectVersion aspectVersion) {
    final String aspectName = aspectVersion.getAspect();
    if (!_supportedMetadataClassNames.contains(aspectName)) {
      throw new UnsupportedAspectClassException(aspectName + " is not a supported metadata type.");
    }

    return ModelUtils.getAspectClass(aspectName);
  }
}
