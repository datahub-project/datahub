package com.linkedin.datahub.graphql.types.ingestion;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IngestionConfig;
import com.linkedin.datahub.graphql.generated.IngestionSchedule;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.ingestion.DataHubIngestionSourceConfig;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Maps {@link EntityResponse} representing an IngestionSource entity to a GraphQL {@link
 * IngestionSource} object.
 */
@Slf4j
public class IngestionSourceMapper implements ModelMapper<EntityResponse, IngestionSource> {
  public static final IngestionSourceMapper INSTANCE = new IngestionSourceMapper();

  /**
   * Maps a {@link EntityResponse} to a GraphQL {@link IngestionSource} object.
   *
   * @param context the query context
   * @param entityResponse the entity response to map
   * @return the mapped GraphQL IngestionSource object
   */
  public static IngestionSource map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  /**
   * Applies the mapping from an {@link EntityResponse} to an {@link IngestionSource}.
   *
   * @param context The query context.
   * @param entityResponse The entity response to map.
   * @return The mapped {@link IngestionSource}.
   */
  @Override
  public IngestionSource apply(
      @Nullable QueryContext context, @Nonnull EntityResponse entityResponse) {
    return mapIngestionSource(context, entityResponse);
  }

  /**
   * Maps an {@link EntityResponse} to an {@link IngestionSource}.
   *
   * @param ingestionSource The entity response to map.
   * @return The mapped {@link IngestionSource}.
   */
  private IngestionSource mapIngestionSource(
      @Nullable QueryContext context, final EntityResponse ingestionSource) {
    final Urn entityUrn = ingestionSource.getUrn();
    final EnvelopedAspectMap aspects = ingestionSource.getAspects();
    final IngestionSource result = new IngestionSource();
    result.setUrn(entityUrn.toString());

    // There should ALWAYS be an info aspect
    // (excepting a case when the source with urn was deleted)
    final EnvelopedAspect envelopedInfo = aspects.get(Constants.INGESTION_INFO_ASPECT_NAME);
    if (envelopedInfo == null) {
      log.warn("No ingestion source info aspect exists for urn: {}", entityUrn);
      return null;
    }

    mapIngestionSourceInfo(result, envelopedInfo);
    mapOwnership(result, context, entityUrn, aspects);

    return result;
  }

  /**
   * Maps {@link EnvelopedAspect} to {@link IngestionSource}.
   *
   * @param result the {@link IngestionSource}
   * @param envelopedInfo the {@link EnvelopedAspect}
   */
  private void mapIngestionSourceInfo(
      final IngestionSource result, final EnvelopedAspect envelopedInfo) {

    final DataHubIngestionSourceInfo info =
        new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());
    result.setName(info.getName());
    result.setType(info.getType());
    result.setConfig(mapIngestionSourceConfig(info.getConfig()));
    if (info.hasSchedule()) {
      result.setSchedule(mapIngestionSourceSchedule(info.getSchedule()));
    }
  }

  /**
   * Maps {@link DataHubIngestionSourceConfig} to {@link IngestionConfig}.
   *
   * @param config the {@link DataHubIngestionSourceConfig}
   * @return the mapped {@link IngestionConfig}
   */
  private IngestionConfig mapIngestionSourceConfig(final DataHubIngestionSourceConfig config) {
    final IngestionConfig result = new IngestionConfig();
    result.setRecipe(config.getRecipe());
    result.setVersion(config.getVersion());
    result.setExecutorId(config.getExecutorId());
    result.setDebugMode(config.isDebugMode());
    if (config.getExtraArgs() != null) {
      List<StringMapEntry> extraArgs =
          config.getExtraArgs().keySet().stream()
              .map(key -> new StringMapEntry(key, config.getExtraArgs().get(key)))
              .collect(Collectors.toList());
      result.setExtraArgs(extraArgs);
    }
    return result;
  }

  /**
   * Maps {@link DataHubIngestionSourceSchedule} to {@link IngestionSchedule}.
   *
   * @param schedule the {@link DataHubIngestionSourceSchedule}
   * @return the mapped {@link IngestionSchedule}
   */
  private IngestionSchedule mapIngestionSourceSchedule(
      final DataHubIngestionSourceSchedule schedule) {
    final IngestionSchedule result = new IngestionSchedule();
    result.setInterval(schedule.getInterval());
    result.setTimezone(schedule.getTimezone());
    return result;
  }

  private void mapOwnership(
      final IngestionSource result,
      @Nullable QueryContext context,
      final Urn urn,
      final EnvelopedAspectMap aspects) {
    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(
          OwnershipMapper.map(context, new Ownership(envelopedOwnership.getValue().data()), urn));
    }
  }
}
