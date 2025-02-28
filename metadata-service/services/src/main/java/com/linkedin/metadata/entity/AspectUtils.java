package com.linkedin.metadata.entity;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.UI_SOURCE;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AspectUtils {

  private AspectUtils() {}

  public static Map<Urn, Aspect> batchGetLatestAspect(
      @Nonnull OperationContext opContext, Set<Urn> urns, String aspectName) {
    final Map<Urn, Map<String, Aspect>> gmsResponse =
        opContext.getAspectRetriever().getLatestAspectObjects(urns, ImmutableSet.of(aspectName));

    return gmsResponse.entrySet().stream()
        .map(
            entry ->
                entry.getValue().containsKey(aspectName)
                    ? Map.entry(entry.getKey(), entry.getValue().get(aspectName))
                    : null)
        .filter(Objects::nonNull)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static MetadataChangeProposal buildMetadataChangeProposal(
      @Nonnull Urn urn, @Nonnull String aspectName, @Nonnull RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  /**
   * Build an MCP that is processed in a fully synchronous manner.
   *
   * <p>This means that the secondary storage will be updated synchronously with the primary
   * storage, without waiting on the MCL kafka topic / eventual consistency.
   */
  public static MetadataChangeProposal buildSynchronousMetadataChangeProposal(
      @Nonnull Urn urn, @Nonnull String aspectName, @Nonnull RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    SystemMetadata systemMetadata = createDefaultSystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    proposal.setSystemMetadata(systemMetadata);
    return proposal;
  }

  public static MetadataChangeProposal buildMetadataChangeProposal(
      @Nonnull String entityType,
      @Nonnull RecordTemplate keyAspect,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityType(entityType);
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(keyAspect));
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  @Nonnull
  public static DataMap getDataMapFromEntityResponse(
      @Nonnull final EntityResponse entityResponse, @Nonnull final String aspectName) {
    Objects.requireNonNull(entityResponse, "entityResponse must not be null");
    Objects.requireNonNull(aspectName, "aspectName must not be null");
    final EnvelopedAspectMap aspects = entityResponse.getAspects();
    if (!aspects.containsKey(aspectName)) {
      throw new RuntimeException(
          String.format("Aspect %s not found in entity response", aspectName));
    }

    final DataMap dataMap = aspects.get(aspectName).getValue().data();
    if (dataMap == null) {
      throw new RuntimeException(String.format("DataMap is null for aspect %s", aspectName));
    }

    return dataMap;
  }

  @Nonnull
  public static Map<Urn, DataMap> getDataMapsFromEntityResponseMap(
      @Nonnull final Map<Urn, EntityResponse> entityResponseMap, @Nonnull final String aspectName) {
    Objects.requireNonNull(entityResponseMap, "entityResponseMap must not be null");
    Objects.requireNonNull(aspectName, "aspectName must not be null");
    return entityResponseMap.entrySet().stream()
        .filter(
            entry ->
                entry.getValue() != null && entry.getValue().getAspects().containsKey(aspectName))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> getDataMapFromEntityResponse(entry.getValue(), aspectName)));
  }

  @Nonnull
  public static EntityResponse createEntityResponseFromAspects(
      @Nonnull final Map<String, RecordTemplate> aspects) {
    final EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    for (Map.Entry<String, RecordTemplate> entry : aspects.entrySet()) {
      aspectMap.put(
          entry.getKey(), new EnvelopedAspect().setValue(new Aspect(entry.getValue().data())));
    }

    final EntityResponse entityResponse = new EntityResponse();
    entityResponse.setAspects(aspectMap);
    return entityResponse;
  }

  public static AspectSpec validateAspect(MetadataChangeLog mcl, EntitySpec entitySpec) {
    if (!mcl.hasAspectName()
        || (!ChangeType.DELETE.equals(mcl.getChangeType()) && !mcl.hasAspect())) {
      throw new UnsupportedOperationException(
          String.format(
              "Aspect and aspect name is required for create and update operations. changeType: %s entityName: %s hasAspectName: %s hasAspect: %s",
              mcl.getChangeType(), entitySpec.getName(), mcl.hasAspectName(), mcl.hasAspect()));
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(mcl.getAspectName());

    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format(
              "Unknown aspect %s for entity %s", mcl.getAspectName(), mcl.getEntityType()));
    }

    return aspectSpec;
  }

  public static AspectSpec validateAspect(MetadataChangeProposal mcp, EntitySpec entitySpec) {
    if (!mcp.hasAspectName() || !mcp.hasAspect()) {
      throw new UnsupportedOperationException(
          "Aspect and aspect name is required for create and update operations");
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(mcp.getAspectName());

    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format(
              "Unknown aspect %s for entity %s", mcp.getAspectName(), mcp.getEntityType()));
    }

    return aspectSpec;
  }
}
