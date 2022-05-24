package com.linkedin.metadata.timeline.differ;

import com.datahub.util.RecordUtils;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.AuditStamp;
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
import com.linkedin.metadata.timeline.data.entity.TagChangeEvent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class GlobalTagsDiffer implements AspectDiffer<GlobalTags> {
  private static final String TAG_ADDED_FORMAT = "Tag '%s' added to entity '%s'.";
  private static final String TAG_REMOVED_FORMAT = "Tag '%s' removed from entity '%s'.";

  public static List<ChangeEvent> computeDiffs(GlobalTags baseGlobalTags, GlobalTags targetGlobalTags,
      String entityUrn, AuditStamp auditStamp) {
    sortGlobalTagsByTagUrn(baseGlobalTags);
    sortGlobalTagsByTagUrn(targetGlobalTags);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    TagAssociationArray baseTags = (baseGlobalTags != null) ? baseGlobalTags.getTags() : new TagAssociationArray();
    TagAssociationArray targetTags =
        (targetGlobalTags != null) ? targetGlobalTags.getTags() : new TagAssociationArray();
    int baseTagIdx = 0;
    int targetTagIdx = 0;
    while (baseTagIdx < baseTags.size() && targetTagIdx < targetTags.size()) {
      TagAssociation baseTagAssociation = baseTags.get(baseTagIdx);
      TagAssociation targetTagAssociation = targetTags.get(targetTagIdx);
      int comparison = baseTagAssociation.getTag().toString().compareTo(targetTagAssociation.getTag().toString());
      if (comparison == 0) {
        // No change to this tag.
        ++baseTagIdx;
        ++targetTagIdx;
      } else if (comparison < 0) {
        // Tag got removed.
        changeEvents.add(TagChangeEvent.entityTagChangeEventBuilder()
            .modifier(baseTagAssociation.getTag().toString())
            .entityUrn(entityUrn)
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.REMOVE)
            .semVerChange(SemanticChangeType.MINOR)
            .description(String.format(TAG_REMOVED_FORMAT, baseTagAssociation.getTag().getId(), entityUrn))
            .tagUrn(baseTagAssociation.getTag())
            .auditStamp(auditStamp)
            .build());
        ++baseTagIdx;
      } else {
        // Tag got added.
        changeEvents.add(TagChangeEvent.entityTagChangeEventBuilder()
            .modifier(targetTagAssociation.getTag().toString())
            .entityUrn(entityUrn)
            .category(ChangeCategory.TAG)
            .operation(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description(String.format(TAG_ADDED_FORMAT, targetTagAssociation.getTag().getId(), entityUrn))
            .tagUrn(targetTagAssociation.getTag())
            .auditStamp(auditStamp)
            .build());
        ++targetTagIdx;
      }
    }

    while (baseTagIdx < baseTags.size()) {
      // Handle removed tags.
      TagAssociation baseTagAssociation = baseTags.get(baseTagIdx);
      changeEvents.add(TagChangeEvent.entityTagChangeEventBuilder()
          .modifier(baseTagAssociation.getTag().toString())
          .entityUrn(entityUrn)
          .category(ChangeCategory.TAG)
          .operation(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(TAG_REMOVED_FORMAT, baseTagAssociation.getTag().getId(), entityUrn))
          .tagUrn(baseTagAssociation.getTag())
          .auditStamp(auditStamp)
          .build());
      ++baseTagIdx;
    }
    while (targetTagIdx < targetTags.size()) {
      // Handle newly added tags.
      TagAssociation targetTagAssociation = targetTags.get(targetTagIdx);
      changeEvents.add(TagChangeEvent.entityTagChangeEventBuilder()
          .modifier(targetTagAssociation.getTag().toString())
          .entityUrn(entityUrn)
          .category(ChangeCategory.TAG)
          .operation(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(String.format(TAG_ADDED_FORMAT, targetTagAssociation.getTag().getId(), entityUrn))
          .tagUrn(targetTagAssociation.getTag())
          .auditStamp(auditStamp)
          .build());
      ++targetTagIdx;
    }
    return changeEvents;
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

    GlobalTags baseGlobalTags = getGlobalTagsFromAspect(previousValue);
    GlobalTags targetGlobalTags = getGlobalTagsFromAspect(currentValue);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.TAG) {
      changeEvents.addAll(computeDiffs(baseGlobalTags, targetGlobalTags, currentValue.getUrn(), null));
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
      @Nonnull Aspect<GlobalTags> from,
      @Nonnull Aspect<GlobalTags> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
