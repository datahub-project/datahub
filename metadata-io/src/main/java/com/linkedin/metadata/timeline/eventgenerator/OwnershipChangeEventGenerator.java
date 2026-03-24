package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.entity.OwnerChangeEvent;
import com.linkedin.util.Pair;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;

public class OwnershipChangeEventGenerator extends EntityChangeEventGenerator<Ownership> {
  private static final String OWNER_ADDED_FORMAT = "'%s' added as a `%s` of '%s'.";
  private static final String OWNER_REMOVED_FORMAT = "'%s' removed as a `%s` of '%s'.";

  private static List<ChangeEvent> computeDiffs(
      Ownership baseOwnership, Ownership targetOwnership, String entityUrn, AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    // Maps (ownership_type, ownership_urn) -> owner urn -> Owner aspect
    Map<Pair<OwnershipType, Urn>, Map<Urn, Owner>> oldOwnershipMap =
        buildOwnershipMap(baseOwnership);
    Map<Pair<OwnershipType, Urn>, Map<Urn, Owner>> newOwnershipMap =
        buildOwnershipMap(targetOwnership);

    Set<Pair<OwnershipType, Urn>> allTypes = new HashSet<>();
    allTypes.addAll(oldOwnershipMap.keySet());
    allTypes.addAll(newOwnershipMap.keySet());

    allTypes.forEach(
        key -> {
          Map<Urn, Owner> oldOwners =
              Optional.ofNullable(oldOwnershipMap.get(key)).orElse(Map.of());
          Map<Urn, Owner> newOwners =
              Optional.ofNullable(newOwnershipMap.get(key)).orElse(Map.of());
          Set<Urn> ownersAdded = new HashSet<>(newOwners.keySet());
          ownersAdded.removeAll(oldOwners.keySet());
          Set<Urn> ownersRemoved = new HashSet<>(oldOwners.keySet());
          ownersRemoved.removeAll(newOwners.keySet());

          ownersAdded.forEach(
              urn ->
                  changeEvents.add(makeAddChangeEvent(newOwners.get(urn), entityUrn, auditStamp)));
          ownersRemoved.forEach(
              urn ->
                  changeEvents.add(
                      makeRemoveChangeEvent(oldOwners.get(urn), entityUrn, auditStamp)));
        });
    return changeEvents;
  }

  private static Ownership getOwnershipFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(Ownership.class, entityAspect.getMetadata());
    }
    return null;
  }

  private static Map<Pair<OwnershipType, Urn>, Map<Urn, Owner>> buildOwnershipMap(
      Ownership ownership) {
    Map<Pair<OwnershipType, Urn>, Map<Urn, Owner>> map = new HashMap<>();

    if (ownership != null) {
      ownership
          .getOwners()
          .forEach(
              owner -> {
                Pair<OwnershipType, Urn> key = new Pair<>(owner.getType(), owner.getTypeUrn());
                if (!map.containsKey(key)) {
                  map.put(key, new HashMap<>());
                }
                map.get(key).put(owner.getOwner(), owner);
              });
    }

    return map;
  }

  private static OwnerChangeEvent makeAddChangeEvent(
      Owner newOwner, String entityUrn, AuditStamp auditStamp) {
    return OwnerChangeEvent.entityOwnerChangeEventBuilder()
        .modifier(newOwner.getOwner().toString())
        .entityUrn(entityUrn)
        .category(ChangeCategory.OWNER)
        .operation(ChangeOperation.ADD)
        .semVerChange(SemanticChangeType.MINOR)
        .description(
            String.format(
                OWNER_ADDED_FORMAT, newOwner.getOwner().getId(), newOwner.getType(), entityUrn))
        .ownerUrn(newOwner.getOwner())
        .ownerType(newOwner.getType())
        .ownerTypeUrn(newOwner.getTypeUrn())
        .auditStamp(auditStamp)
        .build();
  }

  private static OwnerChangeEvent makeRemoveChangeEvent(
      Owner oldOwner, String entityUrn, AuditStamp auditStamp) {
    return OwnerChangeEvent.entityOwnerChangeEventBuilder()
        .modifier(oldOwner.getOwner().toString())
        .entityUrn(entityUrn)
        .category(ChangeCategory.OWNER)
        .operation(ChangeOperation.REMOVE)
        .semVerChange(SemanticChangeType.MINOR)
        .description(
            String.format(
                OWNER_REMOVED_FORMAT, oldOwner.getOwner().getId(), oldOwner.getType(), entityUrn))
        .ownerUrn(oldOwner.getOwner())
        .ownerType(oldOwner.getType())
        .ownerTypeUrn(oldOwner.getTypeUrn())
        .auditStamp(auditStamp)
        .build();
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

    if (!previousValue.getAspect().equals(OWNERSHIP_ASPECT_NAME)
        || !currentValue.getAspect().equals(OWNERSHIP_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + OWNERSHIP_ASPECT_NAME);
    }

    Ownership baseOwnership = getOwnershipFromAspect(previousValue);
    Ownership targetOwnership = getOwnershipFromAspect(currentValue);

    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.OWNER) {
      changeEvents.addAll(
          computeDiffs(baseOwnership, targetOwnership, currentValue.getUrn(), null));
    }

    // Assess the highest change at the transaction(schema) level.
    // Why isn't this done at changeevent level - what if transaction contains multiple category
    // events?
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
