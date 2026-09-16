package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.Constants.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociation;
import com.linkedin.common.MetadataAttribution;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DocumentationChangeEventGenerator extends EntityChangeEventGenerator<Documentation> {

  public static final String DESCRIPTION_ADDED = "Documentation for '%s' has been added: '%s'.";
  public static final String DESCRIPTION_REMOVED = "Documentation for '%s' has been removed: '%s'.";
  public static final String DESCRIPTION_CHANGED =
      "Documentation of '%s' has been changed from '%s' to '%s'.";

  private static List<ChangeEvent> computeDiffs(
      Documentation baseDocumentation,
      Documentation targetDocumentation,
      String entityUrn,
      AuditStamp auditStamp) {
    List<ChangeEvent> changeEvents = new ArrayList<>();

    Map<String, DocumentationAssociation> oldDocMap = buildDocumentationMap(baseDocumentation);
    Map<String, DocumentationAssociation> newDocMap = buildDocumentationMap(targetDocumentation);

    Set<String> allKeys = new HashSet<>();
    allKeys.addAll(oldDocMap.keySet());
    allKeys.addAll(newDocMap.keySet());

    for (String key : allKeys) {
      DocumentationAssociation oldAssoc = oldDocMap.get(key);
      DocumentationAssociation newAssoc = newDocMap.get(key);

      if (oldAssoc == null) {
        changeEvents.add(makeAddChangeEvent(newAssoc, key, entityUrn, auditStamp));
      } else if (newAssoc == null) {
        changeEvents.add(makeRemoveChangeEvent(oldAssoc, key, entityUrn, auditStamp));
      } else if (!oldAssoc.getDocumentation().equals(newAssoc.getDocumentation())) {
        changeEvents.add(makeModifyChangeEvent(oldAssoc, newAssoc, key, entityUrn, auditStamp));
      }
    }

    return changeEvents;
  }

  /**
   * Returns the attribution source URN string as the stable key, or null for unattributed entries
   * (at most one such entry is expected per the aspect semantics).
   */
  @Nullable
  static String getKey(DocumentationAssociation association) {
    MetadataAttribution attribution = association.getAttribution();
    if (attribution != null && attribution.hasSource() && attribution.getSource() != null) {
      return attribution.getSource().toString();
    }
    return null;
  }

  private static Map<String, DocumentationAssociation> buildDocumentationMap(
      Documentation documentation) {
    Map<String, DocumentationAssociation> map = new HashMap<>();
    if (documentation != null) {
      documentation.getDocumentations().forEach(assoc -> map.put(getKey(assoc), assoc));
    }
    return map;
  }

  private static ChangeEvent makeAddChangeEvent(
      DocumentationAssociation assoc, String key, String entityUrn, AuditStamp auditStamp) {
    return ChangeEvent.builder()
        .modifier(key)
        .entityUrn(entityUrn)
        .category(ChangeCategory.DOCUMENTATION)
        .operation(ChangeOperation.ADD)
        .semVerChange(SemanticChangeType.MINOR)
        .description(String.format(DESCRIPTION_ADDED, entityUrn, assoc.getDocumentation()))
        .parameters(ImmutableMap.of("documentation", assoc.getDocumentation()))
        .auditStamp(auditStamp)
        .build();
  }

  private static ChangeEvent makeRemoveChangeEvent(
      DocumentationAssociation assoc, String key, String entityUrn, AuditStamp auditStamp) {
    return ChangeEvent.builder()
        .modifier(key)
        .entityUrn(entityUrn)
        .category(ChangeCategory.DOCUMENTATION)
        .operation(ChangeOperation.REMOVE)
        .semVerChange(SemanticChangeType.MINOR)
        .description(String.format(DESCRIPTION_REMOVED, entityUrn, assoc.getDocumentation()))
        .parameters(ImmutableMap.of("documentation", assoc.getDocumentation()))
        .auditStamp(auditStamp)
        .build();
  }

  private static ChangeEvent makeModifyChangeEvent(
      DocumentationAssociation oldAssoc,
      DocumentationAssociation newAssoc,
      String key,
      String entityUrn,
      AuditStamp auditStamp) {
    return ChangeEvent.builder()
        .modifier(key)
        .entityUrn(entityUrn)
        .category(ChangeCategory.DOCUMENTATION)
        .operation(ChangeOperation.MODIFY)
        .semVerChange(SemanticChangeType.PATCH)
        .description(
            String.format(
                DESCRIPTION_CHANGED,
                entityUrn,
                oldAssoc.getDocumentation(),
                newAssoc.getDocumentation()))
        .parameters(ImmutableMap.of("documentation", newAssoc.getDocumentation()))
        .auditStamp(auditStamp)
        .build();
  }

  @Nullable
  private static Documentation getDocumentationFromAspect(EntityAspect entityAspect) {
    if (entityAspect != null && entityAspect.getMetadata() != null) {
      return RecordUtils.toRecordTemplate(Documentation.class, entityAspect.getMetadata());
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

    if (!previousValue.getAspect().equals(DOCUMENTATION_ASPECT_NAME)
        || !currentValue.getAspect().equals(DOCUMENTATION_ASPECT_NAME)) {
      throw new IllegalArgumentException("Aspect is not " + DOCUMENTATION_ASPECT_NAME);
    }

    List<ChangeEvent> changeEvents = new ArrayList<>();
    if (element == ChangeCategory.DOCUMENTATION) {
      Documentation baseDocumentation = getDocumentationFromAspect(previousValue);
      Documentation targetDocumentation = getDocumentationFromAspect(currentValue);
      changeEvents.addAll(
          computeDiffs(baseDocumentation, targetDocumentation, currentValue.getUrn(), null));
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
      @Nonnull Aspect<Documentation> from,
      @Nonnull Aspect<Documentation> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }
}
