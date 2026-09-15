package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataProductPropertiesChangeEventGenerator
    extends EntityChangeEventGenerator<DataProductProperties> {
  private static final String NAME_ADDED_FORMAT = "Name for '%s' has been set: '%s'.";
  private static final String NAME_REMOVED_FORMAT = "Name for '%s' has been removed: '%s'.";
  private static final String NAME_CHANGED_FORMAT =
      "Name of '%s' has been changed from '%s' to '%s'.";
  private static final String DESCRIPTION_ADDED_FORMAT =
      "Description for '%s' has been added: '%s'.";
  private static final String DESCRIPTION_REMOVED_FORMAT =
      "Description for '%s' has been removed: '%s'.";
  private static final String DESCRIPTION_CHANGED_FORMAT =
      "Description of '%s' has been changed from '%s' to '%s'.";
  private static final String ASSET_ADDED_FORMAT = "Asset '%s' added to data product '%s'.";
  private static final String ASSET_REMOVED_FORMAT = "Asset '%s' removed from data product '%s'.";

  private static List<ChangeEvent> computeDiffs(
      @Nullable DataProductProperties baseProperties,
      @Nonnull DataProductProperties targetProperties,
      @Nonnull String entityUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    // Diff name (optional field on DataProductProperties)
    String baseName = (baseProperties != null) ? baseProperties.getName() : null;
    String targetName = targetProperties.getName();

    if (baseName == null && targetName != null) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(NAME_ADDED_FORMAT, entityUrn, targetName))
              .auditStamp(auditStamp)
              .build());
    } else if (baseName != null && targetName == null) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.REMOVE)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(NAME_REMOVED_FORMAT, entityUrn, baseName))
              .auditStamp(auditStamp)
              .build());
    } else if (baseName != null && targetName != null && !baseName.equals(targetName)) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(NAME_CHANGED_FORMAT, entityUrn, baseName, targetName))
              .auditStamp(auditStamp)
              .build());
    }

    // Diff description (optional field)
    String baseDescription = (baseProperties != null) ? baseProperties.getDescription() : null;
    String targetDescription = targetProperties.getDescription();

    if (baseDescription == null && targetDescription != null) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(DESCRIPTION_ADDED_FORMAT, entityUrn, targetDescription))
              .auditStamp(auditStamp)
              .build());
    } else if (baseDescription != null && targetDescription == null) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.REMOVE)
              .semVerChange(SemanticChangeType.MINOR)
              .description(String.format(DESCRIPTION_REMOVED_FORMAT, entityUrn, baseDescription))
              .auditStamp(auditStamp)
              .build());
    } else if (baseDescription != null
        && targetDescription != null
        && !baseDescription.equals(targetDescription)) {
      changeEvents.add(
          ChangeEvent.builder()
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format(
                      DESCRIPTION_CHANGED_FORMAT, entityUrn, baseDescription, targetDescription))
              .auditStamp(auditStamp)
              .build());
    }

    return changeEvents;
  }

  private static List<ChangeEvent> computeAssetDiffs(
      @Nullable DataProductProperties baseProperties,
      @Nullable DataProductProperties targetProperties,
      @Nonnull String entityUrn,
      AuditStamp auditStamp) {
    List<String> base = extractAssetUrns(baseProperties);
    List<String> target = extractAssetUrns(targetProperties);
    return ChangeEventGeneratorUtils.sortedMergeDiff(
        base,
        target,
        java.util.function.Function.identity(),
        (assetUrn, op) -> buildAssetEvent(assetUrn, entityUrn, op, auditStamp));
  }

  private static List<String> extractAssetUrns(@Nullable DataProductProperties properties) {
    if (properties == null || !properties.hasAssets()) {
      return null;
    }
    DataProductAssociationArray assets = properties.getAssets();
    List<String> urns = new ArrayList<>(assets.size());
    assets.forEach(assoc -> urns.add(assoc.getDestinationUrn().toString()));
    return urns;
  }

  private static ChangeEvent buildAssetEvent(
      String assetUrn, String entityUrn, ChangeOperation operation, AuditStamp auditStamp) {
    String format = (operation == ChangeOperation.ADD) ? ASSET_ADDED_FORMAT : ASSET_REMOVED_FORMAT;
    return ChangeEvent.builder()
        .modifier(assetUrn)
        .entityUrn(entityUrn)
        .category(ChangeCategory.ASSET_MEMBERSHIP)
        .operation(operation)
        .semVerChange(SemanticChangeType.MINOR)
        .description(String.format(format, assetUrn, entityUrn))
        .auditStamp(auditStamp)
        .build();
  }

  @Nullable
  private static DataProductProperties getPropertiesFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(DataProductProperties.class, entityAspect.getMetadata());
    }
    return null;
  }

  @Override
  public ChangeTransaction getSemanticDiff(
      EntityAspect previousValue,
      EntityAspect currentValue,
      ChangeCategory element,
      JsonPatch rawDiff,
      boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
        || !currentValue.getAspect().equals(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
    }
    List<ChangeEvent> changeEvents = new ArrayList<>();
    DataProductProperties baseProperties = getPropertiesFromAspect(previousValue);
    DataProductProperties targetProperties = getPropertiesFromAspect(currentValue);
    if (targetProperties != null) {
      if (element == ChangeCategory.DOCUMENTATION) {
        changeEvents.addAll(
            computeDiffs(baseProperties, targetProperties, currentValue.getUrn(), null));
      } else if (element == ChangeCategory.ASSET_MEMBERSHIP) {
        changeEvents.addAll(
            computeAssetDiffs(baseProperties, targetProperties, currentValue.getUrn(), null));
      }
    }

    SemanticChangeType highestSemanticChange = SemanticChangeType.NONE;
    ChangeEvent highestChangeEvent =
        changeEvents.stream().max(Comparator.comparing(ChangeEvent::getSemVerChange)).orElse(null);
    if (highestChangeEvent != null) {
      highestSemanticChange = highestChangeEvent.getSemVerChange();
    }

    return ChangeTransaction.builder()
        .semVerChange(highestSemanticChange)
        .changeEvents(changeEvents)
        .timestamp(currentValue.getCreatedOn().getTime())
        .rawDiff(rawDiffsRequested ? rawDiff : null)
        .actor(currentValue.getCreatedBy())
        .build();
  }

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<DataProductProperties> from,
      @Nonnull Aspect<DataProductProperties> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
