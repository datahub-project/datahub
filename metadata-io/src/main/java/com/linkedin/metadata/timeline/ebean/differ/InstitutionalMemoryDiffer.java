package com.linkedin.metadata.timeline.ebean.differ;

import com.datahub.util.RecordUtils;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.InstitutionalMemoryMetadata;
import com.linkedin.common.InstitutionalMemoryMetadataArray;
import com.linkedin.common.url.Url;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.linkedin.metadata.Constants.*;


public class InstitutionalMemoryDiffer implements Differ {
  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(INSTITUTIONAL_MEMORY_ASPECT_NAME) || !currentValue.getAspect()
        .equals(INSTITUTIONAL_MEMORY_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + INSTITUTIONAL_MEMORY_ASPECT_NAME);
    }
    assert (currentValue != null);
    InstitutionalMemory baseInstitutionalMemory = getInstitutionalMemoryFromAspect(previousValue);
    InstitutionalMemory targetInstitutionalMemory = getInstitutionalMemoryFromAspect(currentValue);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      changeEvents.addAll(computeDiffs(baseInstitutionalMemory, targetInstitutionalMemory, currentValue.getUrn()));
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

  private List<ChangeEvent> computeDiffs(InstitutionalMemory baseInstitutionalMemory,
      InstitutionalMemory targetInstitutionalMemory, String entityUrn) {
    sortElementsByUrl(baseInstitutionalMemory);
    sortElementsByUrl(targetInstitutionalMemory);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    InstitutionalMemoryMetadataArray baseElements =
        (baseInstitutionalMemory != null) ? baseInstitutionalMemory.getElements()
            : new InstitutionalMemoryMetadataArray();
    InstitutionalMemoryMetadataArray targetElements = targetInstitutionalMemory.getElements();
    int baseIdx = 0;
    int targetIdx = 0;
    while (baseIdx < baseElements.size() && targetIdx < targetElements.size()) {
      InstitutionalMemoryMetadata baseElement = baseElements.get(baseIdx);
      InstitutionalMemoryMetadata targetElement = targetElements.get(targetIdx);
      int comparison = baseElement.getUrl().toString().compareTo(targetElement.getUrl().toString());
      if (comparison == 0) {
        if (!baseElement.getDescription().equals(targetElement.getDescription())) {
          // InstitutionalMemory description has changed.
          changeEvents.add(ChangeEvent.builder()
              .elementId(baseElement.getUrl().toString())
              .target(entityUrn)
              .category(ChangeCategory.DOCUMENTATION)
              .changeType(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format("The institutionalMemory description of the element '%s' changed from '%s' to '%s'.",
                      baseElement.getUrl(), baseElement.getDescription(), targetElement.getDescription()))
              .build());
        }
        ++baseIdx;
        ++targetIdx;
      } else if (comparison < 0) {
        // InstitutionalMemory got removed.
        changeEvents.add(ChangeEvent.builder()
            .elementId(baseElement.getUrl().toString())
            .target(entityUrn)
            .category(ChangeCategory.DOCUMENTATION)
            .changeType(ChangeOperation.REMOVE)
            .semVerChange(SemanticChangeType.MINOR)
            .description(String.format("The institutionalMemory '%s' of the dataset '%s' has been removed.",
                baseElement.getUrl(), entityUrn))
            .build());
        ++baseIdx;
      } else {
        // InstitutionalMemory got added..
        changeEvents.add(ChangeEvent.builder()
            .elementId(targetElement.getUrl().toString())
            .target(entityUrn)
            .category(ChangeCategory.DOCUMENTATION)
            .changeType(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description(String.format("The institutionalMemory '%s' for the dataset '%s' has been added.",
                targetElement.getUrl(), entityUrn))
            .build());
        ++targetIdx;
      }
    }

    while (baseIdx < baseElements.size()) {
      // InstitutionalMemory got removed.
      InstitutionalMemoryMetadata baseElement = baseElements.get(baseIdx);
      changeEvents.add(ChangeEvent.builder()
          .elementId(baseElement.getUrl().toString())
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .description(
              String.format("The institutionalMemory '%s' of the dataset '%s' has been removed.", baseElement.getUrl(),
                  entityUrn))
          .build());
      ++baseIdx;
    }
    while (targetIdx < targetElements.size()) {
      // Newly added owners.
      InstitutionalMemoryMetadata targetElement = targetElements.get(targetIdx);
      // InstitutionalMemory got added..
      changeEvents.add(ChangeEvent.builder()
          .elementId(targetElement.getUrl().toString())
          .target(entityUrn)
          .category(ChangeCategory.DOCUMENTATION)
          .changeType(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(
              String.format("The institutionalMemory '%s' for the dataset '%s' has been added.", targetElement.getUrl(),
                  entityUrn))
          .build());
      ++targetIdx;
    }
    return changeEvents;
  }

  private InstitutionalMemory getInstitutionalMemoryFromAspect(EbeanAspectV2 ebeanAspectV2) {
    if (ebeanAspectV2 != null && ebeanAspectV2.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(InstitutionalMemory.class, ebeanAspectV2.getMetadata());
    }
    return null;
  }

  private void sortElementsByUrl(InstitutionalMemory institutionalMemory) {
    if (institutionalMemory == null) {
      return;
    }
    List<InstitutionalMemoryMetadata> elements = new ArrayList<>(institutionalMemory.getElements());
    elements.sort(Comparator.comparing(InstitutionalMemoryMetadata::getUrl, Comparator.comparing(Url::toString)));
    institutionalMemory.setElements(new InstitutionalMemoryMetadataArray(elements));
  }
}
