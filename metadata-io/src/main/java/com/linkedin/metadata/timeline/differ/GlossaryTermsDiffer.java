package com.linkedin.metadata.timeline.differ;

import com.datahub.util.RecordUtils;
import com.github.fge.jsonpatch.JsonPatch;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
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


public class GlossaryTermsDiffer implements Differ {
  private static final String GLOSSARY_TERM_ADDED_FORMAT = "Term '%s' added to entity '%s'.";
  private static final String GLOSSARY_TERM_REMOVED_FORMAT = "Term '%s' removed from entity '%s'.";

  public static List<ChangeEvent> computeDiffs(GlossaryTerms baseGlossaryTerms, GlossaryTerms targetGlossaryTerms,
      String entityUrn) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    sortGlossaryTermsByGlossaryTermUrn(baseGlossaryTerms);
    sortGlossaryTermsByGlossaryTermUrn(targetGlossaryTerms);

    GlossaryTermAssociationArray baseTerms =
        (baseGlossaryTerms != null) ? baseGlossaryTerms.getTerms() : new GlossaryTermAssociationArray();
    GlossaryTermAssociationArray targetTerms =
        (targetGlossaryTerms != null) ? targetGlossaryTerms.getTerms() : new GlossaryTermAssociationArray();

    int baseGlossaryTermIdx = 0;
    int targetGlossaryTermIdx = 0;
    while (baseGlossaryTermIdx < baseTerms.size() && targetGlossaryTermIdx < targetTerms.size()) {
      GlossaryTermAssociation baseGlossaryTermAssociation = baseTerms.get(baseGlossaryTermIdx);
      GlossaryTermAssociation targetGlossaryTermAssociation = targetTerms.get(targetGlossaryTermIdx);
      int comparison =
          baseGlossaryTermAssociation.getUrn().toString().compareTo(targetGlossaryTermAssociation.getUrn().toString());
      if (comparison == 0) {
        ++baseGlossaryTermIdx;
        ++targetGlossaryTermIdx;
      } else if (comparison < 0) {
        // GlossaryTerm got removed.
        changeEvents.add(ChangeEvent.builder()
            .elementId(baseGlossaryTermAssociation.getUrn().toString())
            .target(entityUrn)
            .category(ChangeCategory.GLOSSARY_TERM)
            .changeType(ChangeOperation.REMOVE)
            .semVerChange(SemanticChangeType.MINOR)
            .description(
                String.format(GLOSSARY_TERM_REMOVED_FORMAT, baseGlossaryTermAssociation.getUrn().getId(), entityUrn))
            .build());
        ++baseGlossaryTermIdx;
      } else {
        // GlossaryTerm got added.
        changeEvents.add(ChangeEvent.builder()
            .elementId(targetGlossaryTermAssociation.getUrn().toString())
            .target(entityUrn)
            .category(ChangeCategory.GLOSSARY_TERM)
            .changeType(ChangeOperation.ADD)
            .semVerChange(SemanticChangeType.MINOR)
            .description(
                String.format(GLOSSARY_TERM_ADDED_FORMAT, targetGlossaryTermAssociation.getUrn().getId(), entityUrn))
            .build());
        ++targetGlossaryTermIdx;
      }
    }

    while (baseGlossaryTermIdx < baseTerms.size()) {
      // Handle removed glossary terms.
      GlossaryTermAssociation baseGlossaryTermAssociation = baseTerms.get(baseGlossaryTermIdx);
      changeEvents.add(ChangeEvent.builder()
          .elementId(baseGlossaryTermAssociation.getUrn().toString())
          .target(entityUrn)
          .category(ChangeCategory.GLOSSARY_TERM)
          .changeType(ChangeOperation.REMOVE)
          .semVerChange(SemanticChangeType.MINOR)
          .description(
              String.format(GLOSSARY_TERM_REMOVED_FORMAT, baseGlossaryTermAssociation.getUrn().getId(), entityUrn))
          .build());
      ++baseGlossaryTermIdx;
    }
    while (targetGlossaryTermIdx < targetTerms.size()) {
      // Handle newly added glossary terms.
      GlossaryTermAssociation targetGlossaryTermAssociation = targetTerms.get(targetGlossaryTermIdx);
      changeEvents.add(ChangeEvent.builder()
          .elementId(targetGlossaryTermAssociation.getUrn().toString())
          .target(entityUrn)
          .category(ChangeCategory.GLOSSARY_TERM)
          .changeType(ChangeOperation.ADD)
          .semVerChange(SemanticChangeType.MINOR)
          .description(
              String.format(GLOSSARY_TERM_ADDED_FORMAT, targetGlossaryTermAssociation.getUrn().getId(), entityUrn))
          .build());
      ++targetGlossaryTermIdx;
    }
    return changeEvents;
  }

  private static void sortGlossaryTermsByGlossaryTermUrn(GlossaryTerms globalGlossaryTerms) {
    if (globalGlossaryTerms == null) {
      return;
    }
    List<GlossaryTermAssociation> glossaryTerms = new ArrayList<>(globalGlossaryTerms.getTerms());
    glossaryTerms.sort(Comparator.comparing(GlossaryTermAssociation::getUrn, Comparator.comparing(Urn::toString)));
    globalGlossaryTerms.setTerms(new GlossaryTermAssociationArray(glossaryTerms));
  }

  private static GlossaryTerms getGlossaryTermsFromAspect(EbeanAspectV2 ebeanAspectV2) {
    if (ebeanAspectV2 != null && ebeanAspectV2.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(GlossaryTerms.class, ebeanAspectV2.getMetadata());
    }
    return null;
  }

  @Override
  public ChangeTransaction getSemanticDiff(EbeanAspectV2 previousValue, EbeanAspectV2 currentValue,
      ChangeCategory element, JsonPatch rawDiff, boolean rawDiffsRequested) {
    if (!previousValue.getAspect().equals(GLOSSARY_TERMS_ASPECT_NAME) || !currentValue.getAspect()
        .equals(GLOSSARY_TERMS_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + GLOSSARY_TERMS_ASPECT_NAME);
    }
    assert (currentValue != null);
    GlossaryTerms baseGlossaryTerms = getGlossaryTermsFromAspect(previousValue);
    GlossaryTerms targetGlossaryTerms = getGlossaryTermsFromAspect(currentValue);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.GLOSSARY_TERM) {
      changeEvents.addAll(computeDiffs(baseGlossaryTerms, targetGlossaryTerms, currentValue.getUrn()));
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
