package com.linkedin.metadata.timeline.differ;

import com.datahub.util.RecordUtils;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.entity.OwnerChangeEvent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;


public class OwnershipDiffer implements AspectDiffer<Ownership> {
  private static final String OWNER_ADDED_FORMAT = "'%s' added as a `%s` of '%s'.";
  private static final String OWNER_REMOVED_FORMAT = "'%s' removed as a `%s` of '%s'.";
  private static final String OWNERSHIP_TYPE_CHANGE_FORMAT =
      "'%s''s ownership type changed from '%s' to '%s' for '%s'.";

  private static List<ChangeEvent> computeDiffs(Ownership baseOwnership, Ownership targetOwnership, String entityUrn,
      AuditStamp auditStamp) {
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
              .modifier(targetOwner.getType().name())
              .entityUrn(baseOwner.getOwner().toString())
              .category(ChangeCategory.OWNER)
              .operation(ChangeOperation.MODIFY)
              .semVerChange(SemanticChangeType.PATCH)
              .description(
                  String.format(OWNERSHIP_TYPE_CHANGE_FORMAT, baseOwner.getOwner().getId(), baseOwner.getType(),
                      targetOwner.getType(), entityUrn))
              .auditStamp(auditStamp)
              .build());
        }
        ++baseOwnerIdx;
        ++targetOwnerIdx;
      } else if (comparison < 0) {
        // Owner got removed
        changeEvents.add(OwnerChangeEvent.entityOwnerChangeEventBuilder()
            .modifier(baseOwner.getOwner().toString())
            .entityUrn(entityUrn)
            .category(ChangeCategory.OWNER)
            .operation(ChangeOperation.REMOVE)
            .semVerChange(SemanticChangeType.MINOR)
            .description(String.format(OWNER_REMOVED_FORMAT, baseOwner.getOwner().getId(), baseOwner.getType(), entityUrn))
            .ownerUrn(baseOwner.getOwner())
            .auditStamp(auditStamp)
            .build());
        ++baseOwnerIdx;
      } else {
        // Owner got added.
        changeEvents.add(OwnerChangeEvent.entityOwnerChangeEventBuilder()
            .modifier(targetOwner.getOwner().toString())
            .entityUrn(entityUrn)
            .category(ChangeCategory.OWNER)
            .operation(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description(String.format(OWNER_ADDED_FORMAT, targetOwner.getOwner().getId(), targetOwner.getType(), entityUrn))
            .ownerUrn(targetOwner.getOwner())
            .auditStamp(auditStamp)
            .build());
        ++targetOwnerIdx;
      }
    }

    while (baseOwnerIdx < baseOwners.size()) {
      // Handle removed owners.
      Owner baseOwner = baseOwners.get(baseOwnerIdx);
      changeEvents.add(OwnerChangeEvent.entityOwnerChangeEventBuilder()
          .modifier(baseOwner.getOwner().toString())
          .entityUrn(entityUrn)
          .category(ChangeCategory.OWNER)
          .operation(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(OWNER_REMOVED_FORMAT, baseOwner.getOwner().getId(), baseOwner.getType(), entityUrn))
          .ownerUrn(baseOwner.getOwner())
          .auditStamp(auditStamp)
          .build());
      ++baseOwnerIdx;
    }
    while (targetOwnerIdx < targetOwners.size()) {
      // Newly added owners.
      Owner targetOwner = targetOwners.get(targetOwnerIdx);
      changeEvents.add(OwnerChangeEvent.entityOwnerChangeEventBuilder()
          .modifier(targetOwner.getOwner().toString())
          .entityUrn(entityUrn)
          .category(ChangeCategory.OWNER)
          .operation(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(OWNER_ADDED_FORMAT, targetOwner.getOwner().getId(), targetOwner.getType(), entityUrn))
          .ownerUrn(targetOwner.getOwner())
          .auditStamp(auditStamp)
          .build());
      ++targetOwnerIdx;
    }
    return changeEvents;
  }

  private static Ownership getOwnershipFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(Ownership.class, entityAspect.getMetadata());
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
  public ChangeTransaction getSemanticDiff(EntityAspect previousValue, EntityAspect currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(OWNERSHIP_ASPECT_NAME) || !currentValue.getAspect()
        .equals(OWNERSHIP_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + OWNERSHIP_ASPECT_NAME);
    }
    assert (currentValue != null);

    Ownership baseOwnership = getOwnershipFromAspect(previousValue);
    Ownership targetOwnership = getOwnershipFromAspect(currentValue);

    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.OWNER) {
      changeEvents.addAll(computeDiffs(baseOwnership, targetOwnership, currentValue.getUrn(), null));
    }

    // Assess the highest change at the transaction(schema) level.
    // Why isn't this done at changeevent level - what if transaction contains multiple category events?
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
      @Nonnull Aspect<Ownership> from,
      @Nonnull Aspect<Ownership> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
