package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.aspect.AspectVersionArray;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.SnapshotKey;
import com.linkedin.restli.client.CreateRequestBuilder;
import com.linkedin.restli.client.GetRequestBuilder;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.ResourceMethod;
import com.linkedin.restli.common.ResourceSpec;
import com.linkedin.restli.common.ResourceSpecImpl;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * A base class for generating rest.li requests against entity-specific snapshot resources.
 *
 * See http://go/gma for more details.
 *
 * @param <SNAPSHOT> must be a valid snapshot type defined in com.linkedin.metadata.snapshot
 * @param <URN> must be the URN type used in {@code SNAPSHOT}
 *
 * @deprecated Use {@link BaseActionRequestBuilder} instead
 */
public abstract class BaseSnapshotRequestBuilder<SNAPSHOT extends RecordTemplate, URN extends Urn>
    extends BaseRequestBuilder<SNAPSHOT, URN> {

  private final Class<SNAPSHOT> _snapshotClass;
  private final Class<URN> _urnClass;
  private final String _baseUriTemplate;
  private final ResourceSpec _resourceSpec;

  protected BaseSnapshotRequestBuilder(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<URN> urnClass,
      @Nonnull String baseUriTemplate) {

    ModelUtils.validateSnapshotUrn(snapshotClass, urnClass);

    _snapshotClass = snapshotClass;
    _urnClass = urnClass;
    _baseUriTemplate = baseUriTemplate;

    _resourceSpec = new ResourceSpecImpl(EnumSet.of(ResourceMethod.GET, ResourceMethod.CREATE), Collections.emptyMap(),
        Collections.emptyMap(), ComplexResourceKey.class, SnapshotKey.class, EmptyRecord.class, snapshotClass,
        Collections.emptyMap());
  }

  @Override
  @Nonnull
  public Class<URN> urnClass() {
    return _urnClass;
  }

  @Override
  @Nonnull
  public Request<SNAPSHOT> getRequest(@Nonnull String aspectName, @Nonnull URN urn, long version) {
    return getRequest(Collections.singleton(new AspectVersion().setAspect(aspectName).setVersion(version)), urn);
  }

  @Override
  @Nonnull
  public Request<SNAPSHOT> getRequest(@Nonnull Set<AspectVersion> aspectVersions, @Nonnull URN urn) {
    final SnapshotKey snapshotKey = new SnapshotKey().setAspectVersions(new AspectVersionArray(aspectVersions));
    return getRequestBuilder(urn).id(new ComplexResourceKey<>(snapshotKey, new EmptyRecord())).build();
  }

  @Nonnull
  private GetRequestBuilder<ComplexResourceKey<SnapshotKey, EmptyRecord>, SNAPSHOT> getRequestBuilder(
      @Nonnull URN urn) {

    final GetRequestBuilder<ComplexResourceKey<SnapshotKey, EmptyRecord>, SNAPSHOT> builder =
        new GetRequestBuilder<>(_baseUriTemplate, _snapshotClass, _resourceSpec, RestliRequestOptions.DEFAULT_OPTIONS);

    pathKeys(urn).forEach(builder::pathKey);

    return builder;
  }

  @Override
  @Nonnull
  public Request createRequest(@Nonnull URN urn, @Nonnull SNAPSHOT snapshot) {
    return createRequestBuilder(urn, snapshot).build();
  }

  @Nonnull
  private CreateRequestBuilder<ComplexResourceKey<SnapshotKey, EmptyRecord>, SNAPSHOT> createRequestBuilder(
      @Nonnull URN urn, @Nonnull SNAPSHOT snapshot) {

    final CreateRequestBuilder<ComplexResourceKey<SnapshotKey, EmptyRecord>, SNAPSHOT> builder =
        new CreateRequestBuilder<>(_baseUriTemplate, _snapshotClass, _resourceSpec,
            RestliRequestOptions.DEFAULT_OPTIONS);

    builder.input(snapshot);
    pathKeys(urn).forEach(builder::pathKey);

    return builder;
  }
}
