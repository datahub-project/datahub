package com.linkedin.metadata.timeline.eventgenerator;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.metadata.aspect.EntityAspect;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.timeline.data.ChangeTransaction;
import com.linkedin.metadata.timeline.data.SemanticChangeType;
import jakarta.json.JsonPatch;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Generates change events for DocumentInfo aspect. Tracks changes to: - Document creation -
 * Content/title modifications - Parent document changes - Related entities (assets and documents) -
 * Document state changes
 */
@Slf4j
public class DocumentInfoChangeEventGenerator extends EntityChangeEventGenerator<DocumentInfo> {

  private static final String CONTENT_CATEGORY = "DOCUMENTATION";
  private static final String PARENT_CATEGORY = "PARENT_DOCUMENT";
  private static final String RELATED_ENTITIES_CATEGORY = "RELATED_ENTITIES";
  private static final String STATE_CATEGORY = "STATUS";
  private static final String LIFECYCLE_CATEGORY = "LIFECYCLE";

  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<DocumentInfo> from,
      @Nonnull Aspect<DocumentInfo> to,
      @Nonnull AuditStamp auditStamp) {
    // This method is required by the interface but not used in our implementation.
    // We use getSemanticDiff instead which is called by the Timeline Service.
    return new ArrayList<>();
  }

  @Override
  public ChangeTransaction getSemanticDiff(
      @Nullable EntityAspect previousValue,
      @Nonnull EntityAspect currentValue,
      @Nonnull ChangeCategory changeCategory,
      @Nullable JsonPatch rawDiff,
      boolean rawDiffsRequested) {

    if (previousValue == null || previousValue.getVersion() == -1) {
      // Document creation
      return getChangeTransactionForCreation(currentValue);
    }

    List<ChangeEvent> changeEvents = new ArrayList<>();
    DocumentInfo oldDoc = getDocumentInfoFromAspect(previousValue);
    DocumentInfo newDoc = getDocumentInfoFromAspect(currentValue);
    AuditStamp auditStamp = getAuditStamp(currentValue);

    if (oldDoc == null || newDoc == null) {
      log.warn(
          "Failed to deserialize DocumentInfo aspect for entity {}, skipping diff",
          currentValue.getUrn());
      return ChangeTransaction.builder()
          .changeEvents(changeEvents)
          .timestamp(currentValue.getCreatedOn().getTime())
          .semVerChange(SemanticChangeType.NONE)
          .build();
    }

    String entityUrn = currentValue.getUrn();

    // Check content/title changes
    if (shouldCheckCategory(changeCategory, CONTENT_CATEGORY)) {
      addContentChanges(oldDoc, newDoc, entityUrn, auditStamp, changeEvents);
    }

    // Check parent document changes
    if (shouldCheckCategory(changeCategory, PARENT_CATEGORY)) {
      addParentChanges(oldDoc, newDoc, entityUrn, auditStamp, changeEvents);
    }

    // Check related entities changes
    if (shouldCheckCategory(changeCategory, RELATED_ENTITIES_CATEGORY)) {
      addRelatedEntitiesChanges(oldDoc, newDoc, entityUrn, auditStamp, changeEvents);
    }

    // Check state changes
    if (shouldCheckCategory(changeCategory, STATE_CATEGORY)) {
      addStateChanges(oldDoc, newDoc, entityUrn, auditStamp, changeEvents);
    }

    return ChangeTransaction.builder()
        .changeEvents(changeEvents)
        .timestamp(currentValue.getCreatedOn().getTime())
        .semVerChange(SemanticChangeType.NONE)
        .build();
  }

  private ChangeTransaction getChangeTransactionForCreation(@Nonnull EntityAspect currentValue) {
    DocumentInfo doc = getDocumentInfoFromAspect(currentValue);
    AuditStamp auditStamp = getAuditStamp(currentValue);
    if (doc == null) {
      return ChangeTransaction.builder()
          .changeEvents(new ArrayList<>())
          .timestamp(currentValue.getCreatedOn().getTime())
          .semVerChange(SemanticChangeType.NONE)
          .build();
    }
    String description =
        String.format("Document '%s' was created", doc.hasTitle() ? doc.getTitle() : "Untitled");

    ChangeEvent createEvent =
        ChangeEvent.builder()
            .category(ChangeCategory.LIFECYCLE)
            .operation(ChangeOperation.CREATE)
            .entityUrn(currentValue.getUrn())
            .auditStamp(auditStamp)
            .description(description)
            .build();

    List<ChangeEvent> events = new ArrayList<>();
    events.add(createEvent);

    return ChangeTransaction.builder()
        .changeEvents(events)
        .timestamp(currentValue.getCreatedOn().getTime())
        .semVerChange(SemanticChangeType.NONE)
        .build();
  }

  private void addContentChanges(
      DocumentInfo oldDoc,
      DocumentInfo newDoc,
      String entityUrn,
      AuditStamp auditStamp,
      List<ChangeEvent> events) {

    // Check title change
    String oldTitle = oldDoc.hasTitle() ? oldDoc.getTitle() : null;
    String newTitle = newDoc.hasTitle() ? newDoc.getTitle() : null;
    if (!Objects.equals(oldTitle, newTitle)) {
      String description =
          String.format("Document title changed from '%s' to '%s'", oldTitle, newTitle);
      events.add(
          ChangeEvent.builder()
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.MODIFY)
              .entityUrn(entityUrn)
              .auditStamp(auditStamp)
              .description(description)
              .parameters(
                  Map.of(
                      "oldTitle",
                      oldTitle != null ? oldTitle : "",
                      "newTitle",
                      newTitle != null ? newTitle : ""))
              .build());
    }

    // Check content change
    String oldContent = oldDoc.hasContents() ? oldDoc.getContents().getText() : null;
    String newContent = newDoc.hasContents() ? newDoc.getContents().getText() : null;
    if (!Objects.equals(oldContent, newContent)) {
      String description = "Document text content was modified";
      events.add(
          ChangeEvent.builder()
              .category(ChangeCategory.DOCUMENTATION)
              .operation(ChangeOperation.MODIFY)
              .entityUrn(entityUrn)
              .auditStamp(auditStamp)
              .description(description)
              .parameters(
                  Map.of(
                      "oldContent",
                      oldContent != null ? oldContent : "",
                      "newContent",
                      newContent != null ? newContent : ""))
              .build());
    }
  }

  private void addParentChanges(
      DocumentInfo oldDoc,
      DocumentInfo newDoc,
      String entityUrn,
      AuditStamp auditStamp,
      List<ChangeEvent> events) {

    Urn oldParent = oldDoc.hasParentDocument() ? oldDoc.getParentDocument().getDocument() : null;
    Urn newParent = newDoc.hasParentDocument() ? newDoc.getParentDocument().getDocument() : null;

    if (!Objects.equals(oldParent, newParent)) {
      String description;
      Map<String, Object> params = new HashMap<>();

      if (oldParent == null && newParent != null) {
        description = String.format("Document moved to parent %s", newParent);
        params.put("newParent", newParent.toString());
      } else if (oldParent != null && newParent == null) {
        description = "Document moved to root level (no parent)";
        params.put("oldParent", oldParent.toString());
      } else {
        description = String.format("Document moved from %s to %s", oldParent, newParent);
        params.put("oldParent", oldParent != null ? oldParent.toString() : "");
        params.put("newParent", newParent != null ? newParent.toString() : "");
      }

      events.add(
          ChangeEvent.builder()
              .category(ChangeCategory.PARENT)
              .operation(ChangeOperation.MODIFY)
              .entityUrn(entityUrn)
              .auditStamp(auditStamp)
              .description(description)
              .parameters(params)
              .build());
    }
  }

  private void addRelatedEntitiesChanges(
      DocumentInfo oldDoc,
      DocumentInfo newDoc,
      String entityUrn,
      AuditStamp auditStamp,
      List<ChangeEvent> events) {

    // Check related assets changes
    List<Urn> oldAssets =
        oldDoc.hasRelatedAssets()
            ? oldDoc.getRelatedAssets().stream()
                .map(asset -> asset.getAsset())
                .collect(Collectors.toList())
            : new ArrayList<>();
    List<Urn> newAssets =
        newDoc.hasRelatedAssets()
            ? newDoc.getRelatedAssets().stream()
                .map(asset -> asset.getAsset())
                .collect(Collectors.toList())
            : new ArrayList<>();

    addRelationshipChanges(oldAssets, newAssets, "asset", "assets", entityUrn, auditStamp, events);

    // Check related documents changes
    List<Urn> oldDocs =
        oldDoc.hasRelatedDocuments()
            ? oldDoc.getRelatedDocuments().stream()
                .map(doc -> doc.getDocument())
                .collect(Collectors.toList())
            : new ArrayList<>();
    List<Urn> newDocs =
        newDoc.hasRelatedDocuments()
            ? newDoc.getRelatedDocuments().stream()
                .map(doc -> doc.getDocument())
                .collect(Collectors.toList())
            : new ArrayList<>();

    addRelationshipChanges(
        oldDocs, newDocs, "document", "documents", entityUrn, auditStamp, events);
  }

  private void addRelationshipChanges(
      List<Urn> oldUrns,
      List<Urn> newUrns,
      String singular,
      String plural,
      String entityUrn,
      AuditStamp auditStamp,
      List<ChangeEvent> events) {

    List<Urn> added = new ArrayList<>(newUrns);
    added.removeAll(oldUrns);

    List<Urn> removed = new ArrayList<>(oldUrns);
    removed.removeAll(newUrns);

    for (Urn urn : added) {
      events.add(
          ChangeEvent.builder()
              .category(ChangeCategory.RELATED_ENTITIES)
              .operation(ChangeOperation.ADD)
              .entityUrn(entityUrn)
              .modifier(urn.toString())
              .auditStamp(auditStamp)
              .description(String.format("Related %s %s was added", singular, urn))
              .build());
    }

    for (Urn urn : removed) {
      events.add(
          ChangeEvent.builder()
              .category(ChangeCategory.RELATED_ENTITIES)
              .operation(ChangeOperation.REMOVE)
              .entityUrn(entityUrn)
              .modifier(urn.toString())
              .auditStamp(auditStamp)
              .description(String.format("Related %s %s was removed", singular, urn))
              .build());
    }
  }

  private void addStateChanges(
      DocumentInfo oldDoc,
      DocumentInfo newDoc,
      String entityUrn,
      AuditStamp auditStamp,
      List<ChangeEvent> events) {

    String oldState = oldDoc.hasStatus() ? oldDoc.getStatus().getState().name() : null;
    String newState = newDoc.hasStatus() ? newDoc.getStatus().getState().name() : null;

    if (!Objects.equals(oldState, newState)) {
      String description =
          String.format("Document state changed from %s to %s", oldState, newState);
      events.add(
          ChangeEvent.builder()
              .category(ChangeCategory.LIFECYCLE)
              .operation(ChangeOperation.MODIFY)
              .entityUrn(entityUrn)
              .auditStamp(auditStamp)
              .description(description)
              .parameters(
                  Map.of(
                      "oldState",
                      oldState != null ? oldState : "",
                      "newState",
                      newState != null ? newState : ""))
              .build());
    }
  }

  @Nullable
  private DocumentInfo getDocumentInfoFromAspect(@Nonnull EntityAspect aspect) {
    try {
      return RecordUtils.toRecordTemplate(DocumentInfo.class, aspect.getMetadata());
    } catch (Exception e) {
      log.error("Failed to deserialize DocumentInfo from aspect", e);
      return null;
    }
  }

  private AuditStamp getAuditStamp(@Nonnull EntityAspect aspect) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(aspect.getCreatedOn().getTime());
    try {
      auditStamp.setActor(Urn.createFromString(aspect.getCreatedBy()));
    } catch (Exception e) {
      log.warn("Failed to parse actor URN: {}", aspect.getCreatedBy());
    }
    return auditStamp;
  }

  private boolean shouldCheckCategory(ChangeCategory requested, String categoryName) {
    // If requested is null, check all categories
    if (requested == null) {
      return true;
    }
    // Map our custom categories to standard ones
    return requested.name().equals(categoryName)
        || (categoryName.equals(CONTENT_CATEGORY) && requested == ChangeCategory.DOCUMENTATION)
        || (categoryName.equals(STATE_CATEGORY) && requested == ChangeCategory.LIFECYCLE)
        || (categoryName.equals(PARENT_CATEGORY) && requested == ChangeCategory.PARENT)
        || (categoryName.equals(RELATED_ENTITIES_CATEGORY)
            && requested == ChangeCategory.RELATED_ENTITIES);
  }
}
