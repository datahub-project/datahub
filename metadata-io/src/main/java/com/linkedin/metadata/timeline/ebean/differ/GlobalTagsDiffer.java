package com.linkedin.metadata.timeline.ebean.differ;

import com.datahub.util.RecordUtils;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
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


public class GlobalTagsDiffer implements Differ {
  public static List<ChangeEvent> computeDiffs(GlobalTags baseGlobalTags, GlobalTags targetGlobalTags,
      String entityUrn) {
    sortGlobalTagsByTagUrn(baseGlobalTags);
    sortGlobalTagsByTagUrn(targetGlobalTags);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    TagAssociationArray baseTags = (baseGlobalTags != null) ? baseGlobalTags.getTags() : new TagAssociationArray();
    TagAssociationArray targetTags = targetGlobalTags.getTags();
    int baseTagIdx = 0;
    int targetTagIdx = 0;
    while (baseTagIdx < baseTags.size() && targetTagIdx < targetTags.size()) {
      TagAssociation baseTagAssociation = baseTags.get(baseTagIdx);
      TagAssociation targetTagAssociation = targetTags.get(targetTagIdx);
      int comparison = baseTagAssociation.getTag().toString().compareTo(targetTagAssociation.getTag().toString());
      if (comparison == 0) {
        ++baseTagIdx;
        ++targetTagIdx;
      } else if (comparison < 0) {
        // Tag got removed.
        changeEvents.add(ChangeEvent.builder()
            .elementId(baseTagAssociation.getTag().toString())
            .target(entityUrn)
            .category(ChangeCategory.TAG)
            .changeType(ChangeOperation.REMOVE)
            .semVerChange(SemanticChangeType.MINOR)
            .description(
                String.format("Tag '%s' of the entity '%s' has been removed.", baseTagAssociation.getTag().getId(),
                    entityUrn))
            .build());
        ++baseTagIdx;
      } else {
        // Tag got added.
        changeEvents.add(ChangeEvent.builder()
            .elementId(targetTagAssociation.getTag().toString())
            .target(entityUrn)
            .category(ChangeCategory.TAG)
            .changeType(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description(String.format("A new tag '%s' for the entity '%s' has been added.",
                targetTagAssociation.getTag().getId(), entityUrn))
            .build());
        ++targetTagIdx;
      }
    }

    while (baseTagIdx < baseTags.size()) {
      // Handle removed tags.
      TagAssociation baseTagAssociation = baseTags.get(baseTagIdx);
      changeEvents.add(ChangeEvent.builder()
          .elementId(baseTagAssociation.getTag().toString())
          .target(entityUrn)
          .category(ChangeCategory.TAG)
          .changeType(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .description(
              String.format("Tag '%s' of the entity '%s' has been removed.", baseTagAssociation.getTag().getId(),
                  entityUrn))
          .build());
      ++baseTagIdx;
    }
    while (targetTagIdx < targetTags.size()) {
      // Handle newly added tags.
      TagAssociation targetTagAssociation = targetTags.get(targetTagIdx);
      changeEvents.add(ChangeEvent.builder()
          .elementId(targetTagAssociation.getTag().toString())
          .target(entityUrn)
          .category(ChangeCategory.TAG)
          .changeType(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(
              String.format("A new tag '%s' for the entity '%s' has been added.", targetTagAssociation.getTag().getId(),
                  entityUrn))
          .build());
      ++targetTagIdx;
    }
    return changeEvents.size() > 0 ? changeEvents : null;
  }

  private static void sortGlobalTagsByTagUrn(GlobalTags globalTags) {
    if (globalTags == null) {
      return;
    }
    List<TagAssociation> tags = new ArrayList<>(globalTags.getTags());
    tags.sort(Comparator.comparing(TagAssociation::getTag, Comparator.comparing(Urn::toString)));
    globalTags.setTags(new TagAssociationArray(tags));
  }

  private static GlobalTags getGlobalTagsFromAspect(EbeanAspectV2 ebeanAspectV2) {
    if (ebeanAspectV2 != null && ebeanAspectV2.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(GlobalTags.class, ebeanAspectV2.getMetadata());
    }
    return null;
  }

  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(GLOBAL_TAGS_ASPECT_NAME) || !currentValue.getAspect()
        .equals(GLOBAL_TAGS_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + GLOBAL_TAGS_ASPECT_NAME);
    }
    assert (currentValue != null);
    GlobalTags baseGlobalTags = getGlobalTagsFromAspect(previousValue);
    GlobalTags targetGlobalTags = getGlobalTagsFromAspect(currentValue);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.TAG) {
      changeEvents.addAll(computeDiffs(baseGlobalTags, targetGlobalTags, currentValue.getUrn()));
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
