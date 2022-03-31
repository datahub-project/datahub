package com.linkedin.metadata.timeline.differ;

import com.datahub.util.RecordUtils;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
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


public class OwnershipDiffer implements Differ {
  private static final String OWNER_ADDED_FORMAT = "'%s' added as a `%s` of '%s'.";
  private static final String OWNER_REMOVED_FORMAT = "'%s' removed as a `%s` of '%s'.";
  private static final String OWNERSHIP_TYPE_CHANGE_FORMAT =
      "'%s''s ownership type changed from '%s' to '%s' for '%s'.";

  private static List<ChangeEvent> computeDiffs(Ownership baseOwnership, Ownership targetOwnership, String entityUrn) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    sortOwnersByUrn(baseOwnership);
    sortOwnersByUrn(targetOwnership);
    OwnerArray baseOwners = (baseOwnership != null) ? baseOwnership.getOwners() : new OwnerArray();
    OwnerArray targetOwners = (targetOwnership != null) ? targetOwnership.getOwners() : new OwnerArray();

    int baseOwnerIdx = 0;
    int targetOwnerIdx = 0;
    while (baseOwnerIdx < baseOwners.size() && targetOwnerIdx < targetOwners.size()) {
      Owner baseOwner = baseOwners.get(baseOwnerIdx);
      Owner targetOwner = targetOwners.get(targetOwnerIdx);
      int comparison = baseOwner.getOwner().toString().compareTo(targetOwner.getOwner().toString());
      if (comparison == 0) {
        if (!baseOwner.getType().equals(targetOwner.getType())) {
          // Ownership type has changed.
          changeEvents.add(ChangeEvent.builder()
              .elementId(targetOwner.getType().name())
              .target(baseOwner.getOwner().toString())
              .category(ChangeCategory.OWNERSHIP)
              .changeType(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.PATCH)
              .description(
                  String.format(OWNERSHIP_TYPE_CHANGE_FORMAT, baseOwner.getOwner().getId(), baseOwner.getType(),
                      targetOwner.getType(), entityUrn))
              .build());
        }
        ++baseOwnerIdx;
        ++targetOwnerIdx;
      } else if (comparison < 0) {
        // Owner got removed
        changeEvents.add(ChangeEvent.builder()
            .elementId(baseOwner.getOwner().toString())
            .target(entityUrn)
            .category(ChangeCategory.OWNERSHIP)
            .changeType(ChangeOperation.REMOVE)
            .semVerChange(SemanticChangeType.MINOR)
            .description(String.format(OWNER_REMOVED_FORMAT, baseOwner.getOwner().getId(), baseOwner.getType(), entityUrn))
            .build());
        ++baseOwnerIdx;
      } else {
        // Owner got added.
        changeEvents.add(ChangeEvent.builder()
            .elementId(targetOwner.getOwner().toString())
            .target(entityUrn)
            .category(ChangeCategory.OWNERSHIP)
            .changeType(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description(String.format(OWNER_ADDED_FORMAT, targetOwner.getOwner().getId(), targetOwner.getType(), entityUrn))
            .build());
        ++targetOwnerIdx;
      }
    }

    while (baseOwnerIdx < baseOwners.size()) {
      // Handle removed owners.
      Owner baseOwner = baseOwners.get(baseOwnerIdx);
      changeEvents.add(ChangeEvent.builder()
          .elementId(baseOwner.getOwner().toString())
          .target(entityUrn)
          .category(ChangeCategory.OWNERSHIP)
          .changeType(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(OWNER_REMOVED_FORMAT, baseOwner.getOwner().getId(), baseOwner.getType(), entityUrn))
          .build());
      ++baseOwnerIdx;
    }
    while (targetOwnerIdx < targetOwners.size()) {
      // Newly added owners.
      Owner targetOwner = targetOwners.get(targetOwnerIdx);
      changeEvents.add(ChangeEvent.builder()
          .elementId(targetOwner.getOwner().toString())
          .target(entityUrn)
          .category(ChangeCategory.OWNERSHIP)
          .changeType(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(OWNER_ADDED_FORMAT, targetOwner.getOwner().getId(), targetOwner.getType(), entityUrn))
          .build());
      ++targetOwnerIdx;
    }
    return changeEvents;
  }

  private static Ownership getOwnershipFromAspect(EbeanAspectV2 ebeanAspectV2) {
    if (ebeanAspectV2 != null && ebeanAspectV2.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(Ownership.class, ebeanAspectV2.getMetadata());
    }
    return null;
  }

  private static void sortOwnersByUrn(Ownership ownership) {
    if (ownership == null) {
      return;
    }
    List<Owner> owners = new ArrayList<>(ownership.getOwners());
    owners.sort(Comparator.comparing(Owner::getOwner, Comparator.comparing(Urn::toString)));
    ownership.setOwners(new OwnerArray(owners));
  }

  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(OWNERSHIP_ASPECT_NAME) || !currentValue.getAspect()
        .equals(OWNERSHIP_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + OWNERSHIP_ASPECT_NAME);
    }
    assert (currentValue != null);

    Ownership baseOwnership = getOwnershipFromAspect(previousValue);
    Ownership targetOwnership = getOwnershipFromAspect(currentValue);

    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.OWNERSHIP) {
      changeEvents.addAll(computeDiffs(baseOwnership, targetOwnership, currentValue.getUrn()));
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
}
