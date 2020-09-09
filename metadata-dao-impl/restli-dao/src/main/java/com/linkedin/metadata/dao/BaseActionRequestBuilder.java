package com.linkedin.metadata.dao;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.DataTemplateUtil;
import com.linkedin.data.template.DynamicRecordMetadata;
import com.linkedin.data.template.FieldDef;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.aspect.AspectVersion;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.restli.client.ActionRequestBuilder;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.RestliRequestOptions;
import com.linkedin.restli.common.ActionResponse;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import com.linkedin.restli.common.ResourceSpec;
import com.linkedin.restli.common.ResourceSpecImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * A base class for generating rest.li requests against entity-specific action methods.
 *
 * See http://go/gma for more details.
 *
 * @param <SNAPSHOT> must be a valid snapshot type defined in com.linkedin.metadata.snapshot
 * @param <URN> must be the URN type used in {@code SNAPSHOT}
 */
public abstract class BaseActionRequestBuilder<SNAPSHOT extends RecordTemplate, URN extends Urn>
    extends BaseRequestBuilder<SNAPSHOT, URN> {

  private final Class<SNAPSHOT> _snapshotClass;
  private final Class<URN> _urnClass;
  private final String _baseUriTemplate;
  private final ResourceSpec _resourceSpec;

  private final FieldDef<?> _urnFieldDef;
  private final FieldDef<?> _aspectsFieldDef;
  private final FieldDef<?> _snapshotFieldDef;
  private final FieldDef<?> _valueFieldDef;

  /**
   * Creates a {@link BaseActionRequestBuilder} for Rest.li-based Remote DAO.
   *
   * @param snapshotClass
   * @param urnClass
   * @param baseUriTemplate
   */
  public BaseActionRequestBuilder(@Nonnull Class<SNAPSHOT> snapshotClass, @Nonnull Class<URN> urnClass,
      @Nonnull String baseUriTemplate) {

    ModelUtils.validateSnapshotUrn(snapshotClass, urnClass);

    _snapshotClass = snapshotClass;
    _urnClass = urnClass;
    _baseUriTemplate = baseUriTemplate;

    _urnFieldDef = new FieldDef<>(PARAM_URN, String.class, DataTemplateUtil.getSchema(String.class));
    _aspectsFieldDef = new FieldDef<>(PARAM_ASPECTS, StringArray.class, DataTemplateUtil.getSchema(StringArray.class));
    _snapshotFieldDef = new FieldDef(PARAM_SNAPSHOT, snapshotClass, DataTemplateUtil.getSchema(snapshotClass));
    _valueFieldDef = new FieldDef<>(ActionResponse.VALUE_NAME, snapshotClass, DataTemplateUtil.getSchema(snapshotClass));

    final HashMap<String, DynamicRecordMetadata> actionRequestMetadata = new HashMap<>();
    actionRequestMetadata.put(ACTION_GET_SNAPSHOT,
        new DynamicRecordMetadata(ACTION_GET_SNAPSHOT, Arrays.asList(_urnFieldDef, _aspectsFieldDef)));
    actionRequestMetadata.put(ACTION_INGEST,
        new DynamicRecordMetadata(ACTION_INGEST, Arrays.asList(_snapshotFieldDef)));

    final HashMap<java.lang.String, DynamicRecordMetadata> actionResponseMetadata = new HashMap<>();
    actionResponseMetadata.put(ACTION_GET_SNAPSHOT,
        new DynamicRecordMetadata(ACTION_GET_SNAPSHOT, Arrays.asList(_valueFieldDef)));
    actionResponseMetadata.put(ACTION_INGEST, new DynamicRecordMetadata(ACTION_INGEST, Collections.emptyList()));

    _resourceSpec = new ResourceSpecImpl(Collections.emptySet(), actionRequestMetadata, actionResponseMetadata,
        ComplexResourceKey.class, EmptyRecord.class, EmptyRecord.class, EmptyRecord.class, Collections.emptyMap());
  }

  @Override
  @Nonnull
  public Class<URN> urnClass() {
    return _urnClass;
  }

  @Override
  @Nonnull
  public Request<SNAPSHOT> getRequest(@Nonnull String aspectName, @Nonnull URN urn, long version) {
    final AspectVersion aspectVersion = new AspectVersion().setAspect(aspectName).setVersion(version);
    return getRequest(Collections.singleton(aspectVersion), urn);
  }

  @Override
  @Nonnull
  public Request<SNAPSHOT> getRequest(@Nonnull Set<AspectVersion> aspectVersions, @Nonnull URN urn) {

    // Versions are ignored as the snapshot method always returns the latest version.
    // Should refactor the method signature in the future to reflect that.
    final List<String> aspectNames = aspectVersions.stream().map(av -> av.getAspect()).collect(Collectors.toList());

    final ActionRequestBuilder builder =
        new ActionRequestBuilder(_baseUriTemplate, _snapshotClass, _resourceSpec, RestliRequestOptions.DEFAULT_OPTIONS);
    builder.name(ACTION_GET_SNAPSHOT);
    builder.addParam(_urnFieldDef, urn.toString());
    builder.addParam(_aspectsFieldDef, new StringArray(aspectNames));
    pathKeys(urn).forEach(builder::pathKey);

    return builder.build();
  }

  @Override
  @Nonnull
  public Request createRequest(@Nonnull URN urn, @Nonnull SNAPSHOT snapshot) {

    final ActionRequestBuilder builder =
        new ActionRequestBuilder(_baseUriTemplate, Void.class, _resourceSpec, RestliRequestOptions.DEFAULT_OPTIONS);
    builder.name(ACTION_INGEST);
    builder.addParam(_snapshotFieldDef, snapshot);
    pathKeys(urn).forEach(builder::pathKey);

    return builder.build();
  }
}
