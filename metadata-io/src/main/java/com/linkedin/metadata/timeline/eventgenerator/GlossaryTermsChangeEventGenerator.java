package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import com.linkedin.metadata.timeline.data.entity.GlossaryTermChangeEvent;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.annotation.Nonnull;

public class GlossaryTermsChangeEventGenerator extends EntityChangeEventGenerator<GlossaryTerms> {
  private static final String GLOSSARY_TERM_ADDED_FORMAT = "Term '%s' added to entity '%s'.";
  private static final String GLOSSARY_TERM_REMOVED_FORMAT = "Term '%s' removed from entity '%s'.";

  public static List<ChangeEvent> computeDiffs(
      GlossaryTerms baseGlossaryTerms,
      GlossaryTerms targetGlossaryTerms,
      String entityUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    sortGlossaryTermsByGlossaryTermUrn(baseGlossaryTerms);
    sortGlossaryTermsByGlossaryTermUrn(targetGlossaryTerms);

    GlossaryTermAssociationArray baseTerms =
        (baseGlossaryTerms != null)
            ? baseGlossaryTerms.getTerms()
            : new GlossaryTermAssociationArray();
    GlossaryTermAssociationArray targetTerms =
        (targetGlossaryTerms != null)
            ? targetGlossaryTerms.getTerms()
            : new GlossaryTermAssociationArray();

    int baseGlossaryTermIdx = 0;
    int targetGlossaryTermIdx = 0;
    while (baseGlossaryTermIdx < baseTerms.size() && targetGlossaryTermIdx < targetTerms.size()) {
      GlossaryTermAssociation baseGlossaryTermAssociation = baseTerms.get(baseGlossaryTermIdx);
      GlossaryTermAssociation targetGlossaryTermAssociation =
          targetTerms.get(targetGlossaryTermIdx);
      int comparison =
          baseGlossaryTermAssociation
              .getUrn()
              .toString()
              .compareTo(targetGlossaryTermAssociation.getUrn().toString());
      if (comparison == 0) {
        ++baseGlossaryTermIdx;
        ++targetGlossaryTermIdx;
      } else if (comparison < 0) {
        // GlossaryTerm got removed.
        changeEvents.add(
            GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
                .modifier(baseGlossaryTermAssociation.getUrn().toString())
                .entityUrn(entityUrn)
                .category(ChangeCategory.GLOSSARY_TERM)
                .operation(ChangeOperation.REMOVE)
                .semVerChange(SemanticChangeType.MINOR)
                .description(
                    String.format(
                        GLOSSARY_TERM_REMOVED_FORMAT,
                        baseGlossaryTermAssociation.getUrn().getId(),
                        entityUrn))
                .termUrn(baseGlossaryTermAssociation.getUrn())
                .auditStamp(auditStamp)
                .build());
        ++baseGlossaryTermIdx;
      } else {
        // GlossaryTerm got added.
        changeEvents.add(
            GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
                .modifier(targetGlossaryTermAssociation.getUrn().toString())
                .entityUrn(entityUrn)
                .category(ChangeCategory.GLOSSARY_TERM)
                .operation(ChangeOperation.ADD)
                .semVerChange(SemanticChangeType.MINOR)
                .description(
                    String.format(
                        GLOSSARY_TERM_ADDED_FORMAT,
                        targetGlossaryTermAssociation.getUrn().getId(),
                        entityUrn))
                .termUrn(targetGlossaryTermAssociation.getUrn())
                .auditStamp(auditStamp)
                .build());
        ++targetGlossaryTermIdx;
      }
    }

    while (baseGlossaryTermIdx < baseTerms.size()) {
      // Handle removed glossary terms.
      GlossaryTermAssociation baseGlossaryTermAssociation = baseTerms.get(baseGlossaryTermIdx);
      changeEvents.add(
          GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
              .modifier(baseGlossaryTermAssociation.getUrn().toString())
              .entityUrn(entityUrn)
              .category(ChangeCategory.GLOSSARY_TERM)
              .operation(ChangeOperation.REMOVE)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format(
                      GLOSSARY_TERM_REMOVED_FORMAT,
                      baseGlossaryTermAssociation.getUrn().getId(),
                      entityUrn))
              .termUrn(baseGlossaryTermAssociation.getUrn())
              .auditStamp(auditStamp)
              .build());
      ++baseGlossaryTermIdx;
    }
    while (targetGlossaryTermIdx < targetTerms.size()) {
      // Handle newly added glossary terms.
      GlossaryTermAssociation targetGlossaryTermAssociation =
          targetTerms.get(targetGlossaryTermIdx);
      changeEvents.add(
          GlossaryTermChangeEvent.entityGlossaryTermChangeEventBuilder()
              .modifier(targetGlossaryTermAssociation.getUrn().toString())
              .entityUrn(entityUrn)
              .category(ChangeCategory.GLOSSARY_TERM)
              .operation(ChangeOperation.ADD)
              .semVerChange(SemanticChangeType.MINOR)
              .description(
                  String.format(
                      GLOSSARY_TERM_ADDED_FORMAT,
                      targetGlossaryTermAssociation.getUrn().getId(),
                      entityUrn))
              .termUrn(targetGlossaryTermAssociation.getUrn())
              .auditStamp(auditStamp)
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
    glossaryTerms.sort(
        Comparator.comparing(GlossaryTermAssociation::getUrn, Comparator.comparing(Urn::toString)));
    globalGlossaryTerms.setTerms(new GlossaryTermAssociationArray(glossaryTerms));
  }

  private static GlossaryTerms getGlossaryTermsFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(GlossaryTerms.class, entityAspect.getMetadata());
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

    if (currentValue == null) {
      throw new IllegalArgumentException("EntityAspect currentValue should not be null");
    }

    if (!previousValue.getAspect().equals(GLOSSARY_TERMS_ASPECT_NAME)
        || !currentValue.getAspect().equals(GLOSSARY_TERMS_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + GLOSSARY_TERMS_ASPECT_NAME);
    }

    GlossaryTerms baseGlossaryTerms = getGlossaryTermsFromAspect(previousValue);
    GlossaryTerms targetGlossaryTerms = getGlossaryTermsFromAspect(currentValue);
    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.GLOSSARY_TERM) {
      changeEvents.addAll(
          computeDiffs(baseGlossaryTerms, targetGlossaryTerms, currentValue.getUrn(), null));
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
      @Nonnull Aspect<GlossaryTerms> from,
      @Nonnull Aspect<GlossaryTerms> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
