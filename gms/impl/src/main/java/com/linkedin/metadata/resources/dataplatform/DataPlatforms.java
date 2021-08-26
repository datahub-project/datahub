package com.linkedin.metadata.resources.dataplatform;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataPlatforms.DataPlatform;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.PegasusUtils;
import com.linkedin.metadata.aspect.DataPlatformAspect;
import com.linkedin.metadata.aspect.DataPlatformAspectArray;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.restli.BaseEntityResource;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.snapshot.DataPlatformSnapshot;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PagingContextParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.annotations.RestMethod;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static com.linkedin.metadata.restli.RestliConstants.*;


/**
 * Resource provides information about various data platforms.
 */
@RestLiCollection(name = "dataPlatforms", namespace = "com.linkedin.dataplatform", keyName = "platformName")
public class DataPlatforms extends BaseEntityResource<
    // @formatter:off
    String,
    DataPlatform,
    DataPlatformUrn,
    DataPlatformSnapshot,
    DataPlatformAspect> {
  // @formatter:on

  public DataPlatforms() {
    super(DataPlatformSnapshot.class, DataPlatformAspect.class);
  }

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  /**
   * Get data platform.
   *
   * @param platformName name of the platform.
   * @param aspectNames list of aspects to be retrieved. Null to retrieve all aspects of the dataPlatforms.
   * @return {@link DataPlatform} data platform value.
   */
  @Nonnull
  @Override
  @RestMethod.Get
  public Task<DataPlatform> get(
      @Nonnull String platformName,
      @QueryParam(PARAM_ASPECTS) @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      final Entity entity = _entityService.getEntity(
          new DataPlatformUrn(platformName),
          projectedAspects);
      if (entity != null) {
        return toValue(entity.getValue().getDataPlatformSnapshot());
      }
      throw RestliUtils.resourceNotFoundException();
    });
  }

  /**
   * Get all data platforms.
   *
   * @param pagingContext paging context used for paginating through the results.
   * @return list of all data platforms.
   */
  @RestMethod.GetAll
  public Task<List<DataPlatform>> getAllDataPlatforms(@Nonnull @PagingContextParam(defaultCount = 100) PagingContext pagingContext) {
    return Task.value(_entityService.listLatestAspects(
        "dataPlatform",
        "dataPlatformInfo",
        pagingContext.getStart(),
        pagingContext.getCount())
            .getValues()
            .stream()
            .map(record -> {
              final DataPlatformInfo info = new DataPlatformInfo(record.data());
              final DataPlatform platform = new DataPlatform();
              platform.setDataPlatformInfo(info);
              platform.setName(info.getName());
              return platform;
            })
            .collect(Collectors.toList())
    );
  }

  /**
   * Get the snapshot of data platform.
   *
   * @param urnString data platform urn.
   * @param aspectNames list of aspects to be returned. null, when all aspects are to be returned.
   * @return snapshot of data platform with the requested aspects.
   */
  @Action(name = ACTION_GET_SNAPSHOT)
  @Override
  @Nonnull
  public Task<DataPlatformSnapshot> getSnapshot(@ActionParam(PARAM_URN) @Nonnull String urnString,
      @ActionParam(PARAM_ASPECTS) @Optional @Nullable String[] aspectNames) {
    final Set<String> projectedAspects = aspectNames == null ? Collections.emptySet() : new HashSet<>(
        Arrays.asList(aspectNames).stream().map(PegasusUtils::getAspectNameFromFullyQualifiedName)
            .collect(Collectors.toList()));
    return RestliUtils.toTask(() -> {
      final Entity entity;
      try {
        entity = _entityService.getEntity(
            Urn.createFromString(urnString), projectedAspects);

        if (entity != null) {
          return entity.getValue().getDataPlatformSnapshot();
        }
        throw RestliUtils.resourceNotFoundException();
      } catch (URISyntaxException e) {
        throw new RuntimeException(String.format("Failed to convert urnString %s into an Urn", urnString));
      }
    });  }

  @Nonnull
  @Override
  protected BaseLocalDAO<DataPlatformAspect, DataPlatformUrn> getLocalDAO() {
    throw new UnsupportedOperationException();
  }

  @Nonnull
  @Override
  protected DataPlatformUrn createUrnFromString(@Nonnull String urnString) throws Exception {
    return DataPlatformUrn.deserialize(urnString);
  }

  @Nonnull
  @Override
  protected DataPlatformUrn toUrn(@Nonnull String platformName) {
    return new DataPlatformUrn(platformName);
  }

  @Nonnull
  @Override
  protected String toKey(@Nonnull DataPlatformUrn urn) {
    return urn.getPlatformNameEntity();
  }

  @Nonnull
  @Override
  protected DataPlatform toValue(@Nonnull DataPlatformSnapshot dataPlatformSnapshot) {
    final DataPlatform dataPlatform = new DataPlatform();
    dataPlatform.setName(dataPlatformSnapshot.getUrn().getPlatformNameEntity());
    ModelUtils.getAspectsFromSnapshot(dataPlatformSnapshot).forEach(aspect -> {
      if (aspect instanceof DataPlatformInfo) {
        dataPlatform.setDataPlatformInfo((DataPlatformInfo) aspect);
      }
    });

    return dataPlatform;
  }

  @Nonnull
  @Override
  protected DataPlatformSnapshot toSnapshot(@Nonnull DataPlatform dataPlatform, @Nonnull DataPlatformUrn urn) {
    final DataPlatformSnapshot dataPlatformSnapshot = new DataPlatformSnapshot();
    final DataPlatformAspectArray aspects = new DataPlatformAspectArray();
    dataPlatformSnapshot.setUrn(urn);
    dataPlatformSnapshot.setAspects(aspects);
    if (dataPlatform.getDataPlatformInfo() != null) {
      aspects.add(ModelUtils.newAspectUnion(DataPlatformAspect.class, dataPlatform.getDataPlatformInfo()));
    }
    return dataPlatformSnapshot;
  }
}
