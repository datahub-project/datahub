package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
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

public class InstitutionalMemoryChangeEventGenerator
    extends EntityChangeEventGenerator<InstitutionalMemory> {

  private static final String INSTITUTIONAL_MEMORY_ADDED_FORMAT =
      "Institutional Memory '%s' with documentation of '%s' has been added: '%s'";
  private static final String INSTITUTIONAL_MEMORY_REMOVED_FORMAT =
      "Institutional Memory '%s' with documentation of '%s' has been removed: '%s'";
  private static final String INSTITUTIONAL_MEMORY_MODIFIED_FORMAT =
      "Documentation of Institutional Memory '%s' of  '%s' has been changed from '%s' to '%s'.";

  private static List<ChangeEvent> computeDiffs(
      InstitutionalMemory baseInstitutionalMemory,
      InstitutionalMemory targetInstitutionalMemory,
      String entityUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    sortElementsByUrl(baseInstitutionalMemory);
    sortElementsByUrl(targetInstitutionalMemory);
    InstitutionalMemoryMetadataArray baseElements =
        (baseInstitutionalMemory != null)
            ? baseInstitutionalMemory.getElements()
            : new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadataArray targetElements =
        (targetInstitutionalMemory != null)
            ? targetInstitutionalMemory.getElements()
            : new InstitutionalMemoryMetadataArray();

    int baseIdx = 0;
    int targetIdx = 0;
    while (baseIdx < baseElements.size() && targetIdx < targetElements.size()) {
      InstitutionalMemoryMetadata baseElement = baseElements.get(baseIdx);
      InstitutionalMemoryMetadata targetElement = targetElements.get(targetIdx);
      int comparison = baseElement.getUrl().toString().compareTo(targetElement.getUrl().toString());
      if (comparison == 0) {
        if (!baseElement.getDescription().equals(targetElement.getDescription())) {
          // InstitutionalMemory description has changed.
          changeEvents.add(
              ChangeEvent.builder()
                  .modifier(baseElement.getUrl().toString())
                  .entityUrn(entityUrn)
                  .category(ChangeCategory.DOCUMENTATION)
                  .operation(ChangeOperation.MODIFY)
                  .semVerChange(SemanticChangeType.PATCH)
                  .description(
                      String.format(
                          INSTITUTIONAL_MEMORY_MODIFIED_FORMAT,
                          baseElement.getUrl(),
                          entityUrn,
                          baseElement.getDescription(),
                          targetElement.getDescription()))
                  .auditStamp(auditStamp)
                  .build());
        }
        ++baseIdx;
        ++targetIdx;
      } else if (comparison < 0) {
        // InstitutionalMemory got removed.
        changeEvents.add(
            ChangeEvent.builder()
                .modifier(baseElement.getUrl().toString())
                .entityUrn(entityUrn)
                .category(ChangeCategory.DOCUMENTATION)
                .operation(ChangeOperation.REMOVE)
                .semVerChange(SemanticChangeType.MINOR)
                .description(
                    String.format(
                        INSTITUTIONAL_MEMORY_REMOVED_FORMAT,
                        baseElement.getUrl(),
                        entityUrn,
                        baseElement.getDescription()))
                .auditStamp(auditStamp)
                .build());
        ++baseIdx;
      } else {
        // InstitutionalMemory got added..
        changeEvents.add(
            ChangeEvent.builder()
                .modifier(targetElement.getUrl().toString())
                .entityUrn(entityUrn)
                .category(ChangeCategory.DOCUMENTATION)
                .operation(ChangeOperation.ADD)
                .semVerChange(SemanticChangeType.MINOR)
                .description(
                    String.format(
                        INSTITUTIONAL_MEMORY_ADDED_FORMAT,
                        targetElement.getUrl(),
                        entityUrn,
                        targetElement.getDescription()))
                .auditStamp(auditStamp)
                .build());
        ++targetIdx;
      }
    }

    while (baseIdx < baseElements.size()) {
      // InstitutionalMemory got removed.
      InstitutionalMemoryMetadata baseElement = baseElements.get(baseIdx);
      changeEvents.add(
          ChangeEvent.builder()
              .modifier(baseElement.getUrl().toString())
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.REMOVE)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format(
                      INSTITUTIONAL_MEMORY_REMOVED_FORMAT,
                      baseElement.getUrl(),
                      entityUrn,
                      baseElement.getDescription()))
              .auditStamp(auditStamp)
              .build());
      ++baseIdx;
    }
    while (targetIdx < targetElements.size()) {
      // Newly added owners.
      InstitutionalMemoryMetadata targetElement = targetElements.get(targetIdx);
      // InstitutionalMemory got added..
      changeEvents.add(
          ChangeEvent.builder()
              .modifier(targetElement.getUrl().toString())
              .entityUrn(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format(
                      INSTITUTIONAL_MEMORY_ADDED_FORMAT,
                      targetElement.getUrl(),
                      entityUrn,
                      targetElement.getDescription()))
              .auditStamp(auditStamp)
              .build());
      ++targetIdx;
    }
    return changeEvents;
  }

  private static InstitutionalMemory getInstitutionalMemoryFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(InstitutionalMemory.class, entityAspect.getMetadata());
    }
    return null;
  }

  private static void sortElementsByUrl(InstitutionalMemory institutionalMemory) {
    if (institutionalMemory == null) {
      return;
    }
    List<InstitutionalMemoryMetadata> elements = new ArrayList<>(institutionalMemory.getElements());
    elements.sort(
        Comparator.comparing(
            InstitutionalMemoryMetadata::getUrl, Comparator.comparing(Url::toString)));
    institutionalMemory.setElements(new InstitutionalMemoryMetadataArray(elements));
  }

  @Override
  public ChangeTransaction getSemanticDiff(
      EntityAspect previousValue,
      EntityAspect currentValue,
      ChangeCategory element,
      JsonPatch rawDiff,
      boolean rawDiffsRequested) {

    if (currentValue == null) {
      throw new IllegalArgumentException("EntityAspect currentValue should not be null");
    }

    if (!previousValue.getAspect().equals(INSTITUTIONAL_MEMORY_ASPECT_NAME)
        || !currentValue.getAspect().equals(INSTITUTIONAL_MEMORY_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + INSTITUTIONAL_MEMORY_ASPECT_NAME);
    }

    InstitutionalMemory baseInstitutionalMemory = getInstitutionalMemoryFromAspect(previousValue);
    InstitutionalMemory targetInstitutionalMemory = getInstitutionalMemoryFromAspect(currentValue);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      changeEvents.addAll(
          computeDiffs(
              baseInstitutionalMemory, targetInstitutionalMemory, currentValue.getUrn(), null));
    }

    // Assess the highest change at the transaction(schema) level.
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
      @Nonnull Aspect<InstitutionalMemory> from,
      @Nonnull Aspect<InstitutionalMemory> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
