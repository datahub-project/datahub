package com.linkedin.datahub.graphql.types.service.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.McpServerProperties;
import com.linkedin.datahub.graphql.generated.McpTransport;
import com.linkedin.datahub.graphql.generated.Service;
import com.linkedin.datahub.graphql.generated.ServiceProperties;
import com.linkedin.datahub.graphql.generated.ServiceSubType;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps a GMS Service entity response to the GraphQL Service type. */
public class ServiceMapper {

  /**
   * Maps a GMS entity response to a GraphQL Service.
   *
   * @param context The query context
   * @param entityResponse The GMS entity response
   * @return The mapped Service, or null if required aspects are missing
   */
  @Nullable
  public static Service map(
      @Nonnull final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final Service result = new Service();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.SERVICE);

    // Map service properties
    final EnvelopedAspect envelopedProperties =
        aspects.get(Constants.SERVICE_PROPERTIES_ASPECT_NAME);
    if (envelopedProperties != null) {
      result.setProperties(
          mapServiceProperties(
              new com.linkedin.service.ServiceProperties(envelopedProperties.getValue().data())));
    }

    // Map MCP server properties
    final EnvelopedAspect envelopedMcpProperties =
        aspects.get(Constants.MCP_SERVER_PROPERTIES_ASPECT_NAME);
    if (envelopedMcpProperties != null) {
      result.setMcpServerProperties(
          mapMcpServerProperties(
              new com.linkedin.service.McpServerProperties(
                  envelopedMcpProperties.getValue().data())));
    }

    // Map ownership
    final EnvelopedAspect envelopedOwnership = aspects.get(Constants.OWNERSHIP_ASPECT_NAME);
    if (envelopedOwnership != null) {
      result.setOwnership(
          OwnershipMapper.map(
              context, new Ownership(envelopedOwnership.getValue().data()), entityUrn));
    }

    // Map status
    final EnvelopedAspect envelopedStatus = aspects.get(Constants.STATUS_ASPECT_NAME);
    if (envelopedStatus != null) {
      result.setStatus(StatusMapper.map(context, new Status(envelopedStatus.getValue().data())));
    }

    // Map global tags
    final EnvelopedAspect envelopedTags = aspects.get(Constants.GLOBAL_TAGS_ASPECT_NAME);
    if (envelopedTags != null) {
      result.setTags(
          GlobalTagsMapper.map(
              context, new GlobalTags(envelopedTags.getValue().data()), entityUrn));
    }

    // Map data platform instance
    final EnvelopedAspect envelopedPlatformInstance =
        aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedPlatformInstance != null) {
      final DataMap data = envelopedPlatformInstance.getValue().data();
      final com.linkedin.common.DataPlatformInstance platformInstance =
          new com.linkedin.common.DataPlatformInstance(data);
      result.setPlatform(mapPlatform(platformInstance));
    }

    // Map subTypes (using standard aspect for consistency with other entities)
    final EnvelopedAspect envelopedSubTypes = aspects.get(Constants.SUB_TYPES_ASPECT_NAME);
    if (envelopedSubTypes != null) {
      final com.linkedin.common.SubTypes subTypes =
          new com.linkedin.common.SubTypes(envelopedSubTypes.getValue().data());
      if (subTypes.hasTypeNames() && !subTypes.getTypeNames().isEmpty()) {
        // Use the first subtype as the primary type
        result.setSubType(ServiceSubType.valueOf(subTypes.getTypeNames().get(0)));
      }
    }

    return result;
  }

  private static ServiceProperties mapServiceProperties(
      @Nonnull final com.linkedin.service.ServiceProperties gmsProperties) {
    final ServiceProperties result = new ServiceProperties();
    result.setDisplayName(gmsProperties.getDisplayName());
    if (gmsProperties.hasDescription()) {
      result.setDescription(gmsProperties.getDescription());
    }
    // Note: subType is set from the subTypes aspect, not from serviceProperties
    return result;
  }

  private static McpServerProperties mapMcpServerProperties(
      @Nonnull final com.linkedin.service.McpServerProperties gmsProperties) {
    final McpServerProperties result = new McpServerProperties();
    result.setUrl(gmsProperties.getUrl());
    result.setTransport(McpTransport.valueOf(gmsProperties.getTransport().toString()));
    result.setTimeout((double) gmsProperties.getTimeout());
    if (gmsProperties.hasCustomHeaders()) {
      result.setCustomHeaders(mapCustomHeaders(gmsProperties.getCustomHeaders()));
    }
    return result;
  }

  private static List<StringMapEntry> mapCustomHeaders(@Nonnull final Map<String, String> headers) {
    final List<StringMapEntry> result = new ArrayList<>();
    for (Map.Entry<String, String> entry : headers.entrySet()) {
      final StringMapEntry mapEntry = new StringMapEntry();
      mapEntry.setKey(entry.getKey());
      mapEntry.setValue(entry.getValue());
      result.add(mapEntry);
    }
    return result;
  }

  private static DataPlatform mapPlatform(
      @Nonnull final com.linkedin.common.DataPlatformInstance platformInstance) {
    // Set dummy platform to be resolved by the DataPlatform type resolver
    final DataPlatform partialPlatform = new DataPlatform();
    partialPlatform.setUrn(platformInstance.getPlatform().toString());
    return partialPlatform;
  }

  private ServiceMapper() {}
}
