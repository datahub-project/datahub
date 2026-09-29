package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermUrnArray;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.glossary.GlossaryRelatedTerms;
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

public class GlossaryRelatedTermsChangeEventGenerator
    extends EntityChangeEventGenerator<GlossaryRelatedTerms> {
  private static final String RELATED_TERM_ADDED_FORMAT = "'%s' relationship '%s' added to '%s'.";
  private static final String RELATED_TERM_REMOVED_FORMAT =
      "'%s' relationship '%s' removed from '%s'.";

  private static List<ChangeEvent> computeDiffs(
      @Nullable GlossaryRelatedTerms baseRelatedTerms,
      @Nonnull GlossaryRelatedTerms targetRelatedTerms,
      @Nonnull String entityUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    changeEvents.addAll(
        diffTermArray(
            getTermsOrNull(baseRelatedTerms, RelType.IS_A),
            targetRelatedTerms.getIsRelatedTerms(),
            "Is A",
            entityUrn,
            auditStamp));
    changeEvents.addAll(
        diffTermArray(
            getTermsOrNull(baseRelatedTerms, RelType.HAS_A),
            targetRelatedTerms.getHasRelatedTerms(),
            "Has A",
            entityUrn,
            auditStamp));
    changeEvents.addAll(
        diffTermArray(
            getTermsOrNull(baseRelatedTerms, RelType.VALUES),
            targetRelatedTerms.getValues(),
            "Has Value",
            entityUrn,
            auditStamp));
    changeEvents.addAll(
        diffTermArray(
            getTermsOrNull(baseRelatedTerms, RelType.RELATED),
            targetRelatedTerms.getRelatedTerms(),
            "Is Related To",
            entityUrn,
            auditStamp));

    return changeEvents;
  }

  private enum RelType {
    IS_A,
    HAS_A,
    VALUES,
    RELATED
  }

  @Nullable
  private static GlossaryTermUrnArray getTermsOrNull(
      @Nullable GlossaryRelatedTerms terms, RelType type) {
    if (terms == null) {
      return null;
    }
    switch (type) {
      case IS_A:
        return terms.getIsRelatedTerms();
      case HAS_A:
        return terms.getHasRelatedTerms();
      case VALUES:
        return terms.getValues();
      case RELATED:
        return terms.getRelatedTerms();
      default:
        return null;
    }
  }

  private static List<ChangeEvent> diffTermArray(
      @Nullable GlossaryTermUrnArray baseTerms,
      @Nullable GlossaryTermUrnArray targetTerms,
      @Nonnull String relationshipName,
      @Nonnull String entityUrn,
      AuditStamp auditStamp) {
    List<GlossaryTermUrn> base = baseTerms != null ? new ArrayList<>(baseTerms) : null;
    List<GlossaryTermUrn> target = targetTerms != null ? new ArrayList<>(targetTerms) : null;
    return ChangeEventGeneratorUtils.sortedMergeDiff(
        base,
        target,
        GlossaryTermUrn::toString,
        (term, op) -> buildEvent(term, relationshipName, entityUrn, op, auditStamp));
  }

  private static ChangeEvent buildEvent(
      GlossaryTermUrn termUrn,
      String relationshipName,
      String entityUrn,
      ChangeOperation operation,
      AuditStamp auditStamp) {
    String format =
        (operation == ChangeOperation.ADD)
            ? RELATED_TERM_ADDED_FORMAT
            : RELATED_TERM_REMOVED_FORMAT;
    return ChangeEvent.builder()
        .modifier(termUrn.toString())
        .entityUrn(entityUrn)
        .category(ChangeCategory.GLOSSARY_TERM)
        .operation(operation)
        .semVerChange(SemanticChangeType.MINOR)
        .description(String.format(format, relationshipName, termUrn.getNameEntity(), entityUrn))
        .parameters(
            ImmutableMap.of("termUrn", termUrn.toString(), "relationshipType", relationshipName))
        .auditStamp(auditStamp)
        .build();
  }

  @Nullable
  private static GlossaryRelatedTerms getGlossaryRelatedTermsFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(GlossaryRelatedTerms.class, entityAspect.getMetadata());
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
    if (!previousValue.getAspect().equals(GLOSSARY_RELATED_TERM_ASPECT_NAME)
        || !currentValue.getAspect().equals(GLOSSARY_RELATED_TERM_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + GLOSSARY_RELATED_TERM_ASPECT_NAME);
    }

    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.GLOSSARY_TERM) {
      GlossaryRelatedTerms baseRelatedTerms = getGlossaryRelatedTermsFromAspect(previousValue);
      GlossaryRelatedTerms targetRelatedTerms = getGlossaryRelatedTermsFromAspect(currentValue);
      if (targetRelatedTerms != null) {
        changeEvents.addAll(
            computeDiffs(baseRelatedTerms, targetRelatedTerms, currentValue.getUrn(), null));
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
      @Nonnull Aspect<GlossaryRelatedTerms> from,
      @Nonnull Aspect<GlossaryRelatedTerms> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
