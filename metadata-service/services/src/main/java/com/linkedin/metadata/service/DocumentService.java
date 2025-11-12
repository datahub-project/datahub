package com.linkedin.metadata.service;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.ParentDocument;
import com.linkedin.knowledge.RelatedAsset;
import com.linkedin.knowledge.RelatedAssetArray;
import com.linkedin.knowledge.RelatedDocument;
import com.linkedin.knowledge.RelatedDocumentArray;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DocumentKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for managing Documents.
 *
 * <p>This service handles CRUD operations for documents, including: - Creating new documents with
 * contents and relationships - Updating document contents and relationships - Moving documents
 * within the hierarchy - Searching and listing documents - Deleting documents
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 */
@Slf4j
public class DocumentService {

  private final SystemEntityClient entityClient;

  public DocumentService(@Nonnull SystemEntityClient entityClient) {
    this.entityClient = entityClient;
  }

  /**
   * Creates a new document.
   *
   * @param opContext the operation context
   * @param id optional custom ID (if null, generates a UUID)
   * @param subTypes optional list of document sub-types
   * @param title optional title
   * @param source optional source information for externally ingested documents
   * @param state optional initial state (UNPUBLISHED or PUBLISHED). If draftOfUrn is provided, this
   *     will be forced to UNPUBLISHED.
   * @param text the document text text
   * @param parentDocumentUrn optional parent document URN
   * @param relatedAssetUrns optional list of related asset URNs
   * @param relatedDocumentUrns optional list of related document URNs
   * @param draftOfUrn optional URN of the published document this is a draft of
   * @param actorUrn the URN of the user creating the document
   * @return the URN of the created document
   * @throws Exception if creation fails
   */
  @Nonnull
  public Urn createDocument(
      @Nonnull OperationContext opContext,
      @Nullable String id,
      @Nullable List<String> subTypes,
      @Nullable String title,
      @Nullable com.linkedin.knowledge.DocumentSource source,
      @Nullable com.linkedin.knowledge.DocumentState state,
      @Nonnull String text,
      @Nullable Urn parentDocumentUrn,
      @Nullable List<Urn> relatedAssetUrns,
      @Nullable List<Urn> relatedDocumentUrns,
      @Nullable Urn draftOfUrn,
      @Nonnull Urn actorUrn)
      throws Exception {

    // Generate document URN
    final String documentId = id != null ? id : UUID.randomUUID().toString();
    final Urn documentUrn =
        Urn.createFromString(
            String.format("urn:li:%s:%s", Constants.DOCUMENT_ENTITY_NAME, documentId));

    // Check if document already exists
    if (entityClient.exists(opContext, documentUrn)) {
      throw new IllegalArgumentException(
          String.format("Document with ID %s already exists", documentId));
    }

    // Validate: if draftOfUrn is provided, state must be UNPUBLISHED (or null, which will default
    // to UNPUBLISHED)
    if (draftOfUrn != null && state == com.linkedin.knowledge.DocumentState.PUBLISHED) {
      throw new IllegalArgumentException(
          "Cannot create a draft document with PUBLISHED state. Draft documents must be UNPUBLISHED.");
    }

    // Create document key
    final DocumentKey documentKey = new DocumentKey();
    documentKey.setId(documentId);

    // Create document info
    final DocumentInfo documentInfo = new DocumentInfo();
    if (title != null) {
      documentInfo.setTitle(title, SetMode.IGNORE_NULL);
    }

    // Set source information if provided (for third-party documents)
    if (source != null) {
      documentInfo.setSource(source, SetMode.IGNORE_NULL);
    }

    // Set text
    final DocumentContents documentContents = new DocumentContents();
    documentContents.setText(text);
    documentInfo.setContents(documentContents);

    // Set created audit stamp
    final AuditStamp created = new AuditStamp();
    created.setTime(System.currentTimeMillis());
    created.setActor(actorUrn);
    documentInfo.setCreated(created);

    // Set lastModified audit stamp (same as created for new documents)
    final AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(actorUrn);
    documentInfo.setLastModified(lastModified);

    // Set status (default to UNPUBLISHED if not provided, force UNPUBLISHED if draftOfUrn is set)
    final com.linkedin.knowledge.DocumentStatus status =
        new com.linkedin.knowledge.DocumentStatus();
    com.linkedin.knowledge.DocumentState finalState =
        state != null ? state : com.linkedin.knowledge.DocumentState.UNPUBLISHED;
    if (draftOfUrn != null) {
      finalState = com.linkedin.knowledge.DocumentState.UNPUBLISHED;
    }
    status.setState(finalState);
    documentInfo.setStatus(status, SetMode.IGNORE_NULL);

    // Set draftOf if provided
    if (draftOfUrn != null) {
      final com.linkedin.knowledge.DraftOf draftOf = new com.linkedin.knowledge.DraftOf();
      draftOf.setDocument(draftOfUrn);
      documentInfo.setDraftOf(draftOf, SetMode.IGNORE_NULL);
    }

    // Embed relationships inside DocumentInfo before serializing
    if (parentDocumentUrn != null) {
      final ParentDocument parent = new ParentDocument();
      parent.setDocument(parentDocumentUrn);
      documentInfo.setParentDocument(parent, SetMode.IGNORE_NULL);
    }

    if (relatedAssetUrns != null && !relatedAssetUrns.isEmpty()) {
      final RelatedAssetArray assetsArray = new RelatedAssetArray();
      relatedAssetUrns.forEach(
          assetUrn -> {
            final RelatedAsset relatedAsset = new RelatedAsset();
            relatedAsset.setAsset(assetUrn);
            assetsArray.add(relatedAsset);
          });
      documentInfo.setRelatedAssets(assetsArray, SetMode.IGNORE_NULL);
    }

    if (relatedDocumentUrns != null && !relatedDocumentUrns.isEmpty()) {
      final RelatedDocumentArray documentsArray = new RelatedDocumentArray();
      relatedDocumentUrns.forEach(
          relatedDocumentUrn -> {
            final RelatedDocument relatedDocument = new RelatedDocument();
            relatedDocument.setDocument(relatedDocumentUrn);
            documentsArray.add(relatedDocument);
          });
      documentInfo.setRelatedDocuments(documentsArray, SetMode.IGNORE_NULL);
    }

    // Create MCP for document info with all relationships embedded
    final MetadataChangeProposal infoMcp = new MetadataChangeProposal();
    infoMcp.setEntityUrn(documentUrn);
    infoMcp.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
    infoMcp.setAspectName(Constants.DOCUMENT_INFO_ASPECT_NAME);
    infoMcp.setChangeType(ChangeType.UPSERT);
    infoMcp.setAspect(GenericRecordUtils.serializeAspect(documentInfo));

    // Prepare list of MCPs to ingest
    final List<MetadataChangeProposal> mcps = new java.util.ArrayList<>();
    mcps.add(infoMcp);

    // Create MCP for subTypes if provided
    if (subTypes != null && !subTypes.isEmpty()) {
      final com.linkedin.common.SubTypes subTypesAspect = new com.linkedin.common.SubTypes();
      subTypesAspect.setTypeNames(new com.linkedin.data.template.StringArray(subTypes));

      final MetadataChangeProposal subTypesMcp = new MetadataChangeProposal();
      subTypesMcp.setEntityUrn(documentUrn);
      subTypesMcp.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
      subTypesMcp.setAspectName(Constants.SUB_TYPES_ASPECT_NAME);
      subTypesMcp.setChangeType(ChangeType.UPSERT);
      subTypesMcp.setAspect(GenericRecordUtils.serializeAspect(subTypesAspect));
      mcps.add(subTypesMcp);
    }

    // Ingest the document with all aspects
    entityClient.batchIngestProposals(opContext, mcps, false);

    log.info("Created document {} for user {}", documentUrn, actorUrn);
    return documentUrn;
  }

  /**
   * Gets a document info by URN.
   *
   * @param opContext the operation context
   * @param documentUrn the document URN
   * @return the document info, or null if not found
   * @throws Exception if retrieval fails
   */
  @Nullable
  public DocumentInfo getDocumentInfo(@Nonnull OperationContext opContext, @Nonnull Urn documentUrn)
      throws Exception {

    final EntityResponse response =
        entityClient.getV2(
            opContext,
            Constants.DOCUMENT_ENTITY_NAME,
            documentUrn,
            Set.of(Constants.DOCUMENT_INFO_ASPECT_NAME));

    if (response == null
        || !response.getAspects().containsKey(Constants.DOCUMENT_INFO_ASPECT_NAME)) {
      return null;
    }

    return new DocumentInfo(
        response.getAspects().get(Constants.DOCUMENT_INFO_ASPECT_NAME).getValue().data());
  }

  /**
   * Updates the contents of a document.
   *
   * @param opContext the operation context
   * @param documentUrn the document URN
   * @param text the new text
   * @param title optional updated title
   * @param subTypes optional updated sub-types
   * @throws Exception if update fails
   */
  public void updateDocumentContents(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nullable String text,
      @Nullable String title,
      @Nullable List<String> subTypes,
      @Nonnull Urn actorUrn)
      throws Exception {

    // Get existing info
    final DocumentInfo existingInfo = getDocumentInfo(opContext, documentUrn);
    if (existingInfo == null) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Update text if provided
    if (text != null) {
      final DocumentContents documentContents = new DocumentContents();
      documentContents.setText(text);
      existingInfo.setContents(documentContents);
    }

    // Update title if provided
    if (title != null) {
      existingInfo.setTitle(title, SetMode.IGNORE_NULL);
    }

    // Update lastModified
    final AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(actorUrn);
    existingInfo.setLastModified(lastModified);

    // Prepare list of MCPs to ingest
    final List<MetadataChangeProposal> mcps = new java.util.ArrayList<>();

    // Ingest updated info
    final MetadataChangeProposal infoMcp = new MetadataChangeProposal();
    infoMcp.setEntityUrn(documentUrn);
    infoMcp.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
    infoMcp.setAspectName(Constants.DOCUMENT_INFO_ASPECT_NAME);
    infoMcp.setChangeType(ChangeType.UPSERT);
    infoMcp.setAspect(GenericRecordUtils.serializeAspect(existingInfo));
    mcps.add(infoMcp);

    // Update subTypes if provided
    if (subTypes != null && !subTypes.isEmpty()) {
      final com.linkedin.common.SubTypes subTypesAspect = new com.linkedin.common.SubTypes();
      subTypesAspect.setTypeNames(new com.linkedin.data.template.StringArray(subTypes));

      final MetadataChangeProposal subTypesMcp = new MetadataChangeProposal();
      subTypesMcp.setEntityUrn(documentUrn);
      subTypesMcp.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
      subTypesMcp.setAspectName(Constants.SUB_TYPES_ASPECT_NAME);
      subTypesMcp.setChangeType(ChangeType.UPSERT);
      subTypesMcp.setAspect(GenericRecordUtils.serializeAspect(subTypesAspect));
      mcps.add(subTypesMcp);
    }

    // Batch ingest all proposals
    entityClient.batchIngestProposals(opContext, mcps, false);

    log.info("Updated contents for document {}", documentUrn);
  }

  /**
   * Updates the related entities for a document.
   *
   * @param opContext the operation context
   * @param documentUrn the document URN
   * @param relatedAssetUrns optional list of related asset URNs (null = don't change, empty =
   *     clear)
   * @param relatedDocumentUrns optional list of related document URNs (null = don't change, empty =
   *     clear)
   * @throws Exception if update fails
   */
  public void updateDocumentRelatedEntities(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nullable List<Urn> relatedAssetUrns,
      @Nullable List<Urn> relatedDocumentUrns,
      @Nonnull Urn actorUrn)
      throws Exception {

    // Fetch existing info
    final DocumentInfo info = getDocumentInfo(opContext, documentUrn);
    if (info == null) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Update related assets if provided
    if (relatedAssetUrns != null) {
      if (relatedAssetUrns.isEmpty()) {
        info.removeRelatedAssets();
      } else {
        final RelatedAssetArray assetsArray = new RelatedAssetArray();
        relatedAssetUrns.forEach(
            assetUrn -> {
              final RelatedAsset relatedAsset = new RelatedAsset();
              relatedAsset.setAsset(assetUrn);
              assetsArray.add(relatedAsset);
            });
        info.setRelatedAssets(assetsArray, SetMode.IGNORE_NULL);
      }
    }

    // Update related documents if provided
    if (relatedDocumentUrns != null) {
      if (relatedDocumentUrns.isEmpty()) {
        info.removeRelatedDocuments();
      } else {
        final RelatedDocumentArray documentsArray = new RelatedDocumentArray();
        relatedDocumentUrns.forEach(
            relatedDocumentUrn -> {
              final RelatedDocument relatedDocument = new RelatedDocument();
              relatedDocument.setDocument(relatedDocumentUrn);
              documentsArray.add(relatedDocument);
            });
        info.setRelatedDocuments(documentsArray, SetMode.IGNORE_NULL);
      }
    }

    // Update lastModified
    final AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(actorUrn);
    info.setLastModified(lastModified);

    // Ingest updated info
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(documentUrn);
    mcp.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
    mcp.setAspectName(Constants.DOCUMENT_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(info));

    entityClient.ingestProposal(opContext, mcp, false);

    log.info("Updated related entities for document {}", documentUrn);
  }

  /**
   * Moves a document to a different parent.
   *
   * @param opContext the operation context
   * @param documentUrn the document URN to move
   * @param newParentUrn the new parent URN (null = move to root)
   * @throws Exception if move fails
   */
  public void moveDocument(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nullable Urn newParentUrn,
      @Nonnull Urn actorUrn)
      throws Exception {

    // Verify document exists
    if (!entityClient.exists(opContext, documentUrn)) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Verify new parent exists if provided
    if (newParentUrn != null) {
      if (!entityClient.exists(opContext, newParentUrn)) {
        throw new IllegalArgumentException(
            String.format("Parent Document with URN %s does not exist", newParentUrn));
      }

      // Prevent moving document to itself
      if (documentUrn.equals(newParentUrn)) {
        throw new IllegalArgumentException("Cannot move a Document to itself as parent");
      }

      // Check for circular references
      if (wouldCreateCircularReference(opContext, documentUrn, newParentUrn)) {
        throw new IllegalArgumentException(
            "Cannot move document: would create a circular parent reference");
      }
    }

    // Fetch existing info
    final DocumentInfo info = getDocumentInfo(opContext, documentUrn);
    if (info == null) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Update parent
    if (newParentUrn != null) {
      final ParentDocument parent = new ParentDocument();
      parent.setDocument(newParentUrn);
      info.setParentDocument(parent, SetMode.IGNORE_NULL);
    } else {
      info.removeParentDocument();
    }

    // Update lastModified
    final AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(actorUrn);
    info.setLastModified(lastModified);

    // Ingest updated info
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(documentUrn);
    mcp.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
    mcp.setAspectName(Constants.DOCUMENT_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(info));

    entityClient.ingestProposal(opContext, mcp, false);

    log.info("Moved document {} to parent {}", documentUrn, newParentUrn);
  }

  /**
   * Update the status of a document.
   *
   * @param opContext the operation context
   * @param documentUrn the URN of the document to update
   * @param newState the new state for the document
   * @param actorUrn the URN of the user updating the status
   * @throws Exception if update fails
   */
  public void updateDocumentStatus(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nonnull com.linkedin.knowledge.DocumentState newState,
      @Nonnull Urn actorUrn)
      throws Exception {

    // Verify document exists
    if (!entityClient.exists(opContext, documentUrn)) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Fetch existing info
    final DocumentInfo info = getDocumentInfo(opContext, documentUrn);
    if (info == null) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Update status
    final com.linkedin.knowledge.DocumentStatus status =
        new com.linkedin.knowledge.DocumentStatus();
    status.setState(newState);
    info.setStatus(status, SetMode.IGNORE_NULL);

    // Update lastModified
    final AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(actorUrn);
    info.setLastModified(lastModified);

    // Ingest updated info
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(documentUrn);
    mcp.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
    mcp.setAspectName(Constants.DOCUMENT_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(info));

    entityClient.ingestProposal(opContext, mcp, false);

    log.info("Updated status of document {} to {}", documentUrn, newState);
  }

  /**
   * Updates the sub-type of a document.
   *
   * @param opContext the operation context
   * @param documentUrn the document URN
   * @param subType the new sub-type value
   * @param actorUrn the actor performing the update
   * @throws Exception if update fails
   */
  public void updateDocumentSubType(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nullable String subType,
      @Nonnull Urn actorUrn)
      throws Exception {

    // Verify document exists
    if (!entityClient.exists(opContext, documentUrn)) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Create SubTypes aspect
    final com.linkedin.common.SubTypes subTypesAspect = new com.linkedin.common.SubTypes();
    if (subType != null) {
      subTypesAspect.setTypeNames(
          new com.linkedin.data.template.StringArray(java.util.Collections.singletonList(subType)));
    } else {
      subTypesAspect.setTypeNames(
          new com.linkedin.data.template.StringArray(java.util.Collections.emptyList()));
    }

    // Create metadata change proposal for SubTypes
    final MetadataChangeProposal subTypesMcp = new MetadataChangeProposal();
    subTypesMcp.setEntityUrn(documentUrn);
    subTypesMcp.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
    subTypesMcp.setAspectName(Constants.SUB_TYPES_ASPECT_NAME);
    subTypesMcp.setChangeType(ChangeType.UPSERT);
    subTypesMcp.setAspect(GenericRecordUtils.serializeAspect(subTypesAspect));

    // Also update lastModified timestamp in DocumentInfo
    final DocumentInfo info = getDocumentInfo(opContext, documentUrn);
    if (info != null) {
      final AuditStamp lastModified = new AuditStamp();
      lastModified.setTime(System.currentTimeMillis());
      lastModified.setActor(actorUrn);
      info.setLastModified(lastModified);

      final MetadataChangeProposal infoMcp = new MetadataChangeProposal();
      infoMcp.setEntityUrn(documentUrn);
      infoMcp.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
      infoMcp.setAspectName(Constants.DOCUMENT_INFO_ASPECT_NAME);
      infoMcp.setChangeType(ChangeType.UPSERT);
      infoMcp.setAspect(GenericRecordUtils.serializeAspect(info));

      // Batch ingest both proposals
      entityClient.batchIngestProposals(
          opContext, java.util.Arrays.asList(subTypesMcp, infoMcp), false);
    } else {
      // Just ingest subTypes if info doesn't exist (shouldn't happen)
      entityClient.ingestProposal(opContext, subTypesMcp, false);
    }

    log.info("Updated sub-type for document {} to {}", documentUrn, subType);
  }

  /**
   * Soft deletes a document by setting the Status aspect removed field to true.
   *
   * @param opContext the operation context
   * @param documentUrn the document URN to soft delete
   * @throws Exception if deletion fails
   */
  public void deleteDocument(@Nonnull OperationContext opContext, @Nonnull Urn documentUrn)
      throws Exception {

    // Verify document exists
    if (!entityClient.exists(opContext, documentUrn)) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Soft delete by setting Status aspect removed = true
    final com.linkedin.common.Status status = new com.linkedin.common.Status();
    status.setRemoved(true);

    final MetadataChangeProposal statusProposal = new MetadataChangeProposal();
    statusProposal.setEntityUrn(documentUrn);
    statusProposal.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
    statusProposal.setAspectName(Constants.STATUS_ASPECT_NAME);
    statusProposal.setChangeType(ChangeType.UPSERT);
    statusProposal.setAspect(GenericRecordUtils.serializeAspect(status));

    entityClient.ingestProposal(opContext, statusProposal, false);
    log.info("Soft deleted document {}", documentUrn);
  }

  /**
   * Set ownership for a document.
   *
   * @param opContext the operation context
   * @param documentUrn the document URN
   * @param owners list of owner URNs with their ownership types
   * @param actorUrn the actor performing the operation
   * @throws Exception if setting ownership fails
   */
  public void setDocumentOwnership(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nonnull java.util.List<com.linkedin.common.Owner> owners,
      @Nonnull Urn actorUrn)
      throws Exception {

    // Create Ownership aspect
    final Ownership ownership = new Ownership();
    final OwnerArray ownerArray = new OwnerArray();
    ownerArray.addAll(owners);
    ownership.setOwners(ownerArray);

    // Set last modified
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(actorUrn);
    ownership.setLastModified(auditStamp);

    // Create MCP for ownership
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(documentUrn);
    mcp.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
    mcp.setAspectName(Constants.OWNERSHIP_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(ownership));

    entityClient.ingestProposal(opContext, mcp, false);

    log.info("Set ownership for document {} with {} owners", documentUrn, owners.size());
  }

  /**
   * Searches for documents with filters.
   *
   * @param opContext the operation context
   * @param query search query
   * @param filter optional filter
   * @param sortCriterion optional sort criterion
   * @param start offset
   * @param count number of results
   * @return search result
   * @throws Exception if search fails
   */
  @Nonnull
  public SearchResult searchDocuments(
      @Nonnull OperationContext opContext,
      @Nonnull String query,
      @Nullable Filter filter,
      @Nullable SortCriterion sortCriterion,
      int start,
      int count)
      throws Exception {

    final SortCriterion sort =
        sortCriterion != null
            ? sortCriterion
            : new SortCriterion().setField("createdAt").setOrder(SortOrder.DESCENDING);

    return entityClient.search(
        opContext.withSearchFlags(flags -> flags.setFulltext(true)),
        Constants.DOCUMENT_ENTITY_NAME,
        query,
        filter,
        Collections.singletonList(sort),
        start,
        count);
  }

  /**
   * Builds a filter for parent document.
   *
   * @param parentDocumentUrn the parent document URN
   * @return the filter
   */
  @Nonnull
  public static Filter buildParentDocumentFilter(@Nullable Urn parentDocumentUrn) {
    if (parentDocumentUrn == null) {
      return null;
    }

    final Criterion parentCriterion =
        new Criterion()
            .setField("parentDocument")
            .setValue(parentDocumentUrn.toString())
            .setCondition(Condition.EQUAL);

    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(new CriterionArray(Collections.singletonList(parentCriterion)))));
  }

  /**
   * Checks if moving a document to a new parent would create a circular reference.
   *
   * @param opContext the operation context
   * @param documentUrn the document being moved
   * @param newParentUrn the proposed new parent
   * @return true if a circular reference would be created
   */
  private boolean wouldCreateCircularReference(
      @Nonnull OperationContext opContext, @Nonnull Urn documentUrn, @Nonnull Urn newParentUrn) {

    Set<Urn> visitedParents = new HashSet<>();
    return checkCircularReference(opContext, documentUrn, newParentUrn, visitedParents);
  }

  /**
   * Recursively walks up the parent tree to detect circular references.
   *
   * @param opContext the operation context
   * @param documentUrn the document being moved
   * @param currentParent the current parent being checked
   * @param visitedParents set of already visited parents to prevent infinite loops
   * @return true if a circular reference is detected
   */
  private boolean checkCircularReference(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nullable Urn currentParent,
      @Nonnull Set<Urn> visitedParents) {

    // Base case: no parent, no cycle possible
    if (currentParent == null) {
      return false;
    }

    // Base case: we've already visited this parent (infinite loop protection)
    if (visitedParents.contains(currentParent)) {
      return false;
    }

    // Base case: found the document we're trying to move in the parent chain - cycle detected!
    if (currentParent.equals(documentUrn)) {
      return true;
    }

    // Mark this parent as visited
    visitedParents.add(currentParent);

    try {
      // Get the parent's document info
      DocumentInfo parentInfo = getDocumentInfo(opContext, currentParent);
      if (parentInfo != null && parentInfo.hasParentDocument()) {
        // Recursively check the parent's parent
        Urn grandParent = parentInfo.getParentDocument().getDocument();
        return checkCircularReference(opContext, documentUrn, grandParent, visitedParents);
      }
    } catch (Exception e) {
      // If we can't get parent info, assume no cycle for safety
      log.warn("Failed to check parent info for {}: {}", currentParent, e.getMessage());
    }

    // No parent found, no cycle
    return false;
  }

  /**
   * Merge a draft document into its parent (the document it is a draft of). This copies the draft's
   * content to the published document and optionally deletes the draft.
   *
   * @param opContext the operation context
   * @param draftUrn the URN of the draft document to merge
   * @param deleteDraft whether to delete the draft after merging (default: true)
   * @param actorUrn the URN of the user performing the merge
   * @throws Exception if merge fails
   */
  public void mergeDraftIntoParent(
      @Nonnull OperationContext opContext,
      @Nonnull Urn draftUrn,
      boolean deleteDraft,
      @Nonnull Urn actorUrn)
      throws Exception {

    // Get draft document info
    DocumentInfo draftInfo = getDocumentInfo(opContext, draftUrn);
    if (draftInfo == null) {
      throw new IllegalArgumentException(
          String.format("Draft document %s does not exist", draftUrn));
    }

    // Verify this is a draft
    if (!draftInfo.hasDraftOf()) {
      throw new IllegalArgumentException(
          String.format("Document %s is not a draft (draftOf field not set)", draftUrn));
    }

    // Get the published document URN
    Urn publishedUrn = draftInfo.getDraftOf().getDocument();

    // Get published document info
    DocumentInfo publishedInfo = getDocumentInfo(opContext, publishedUrn);
    if (publishedInfo == null) {
      throw new IllegalArgumentException(
          String.format("Published document %s does not exist", publishedUrn));
    }

    // Copy draft content to published document (preserving published document's draftOf=null)
    publishedInfo.setContents(draftInfo.getContents());
    if (draftInfo.hasTitle()) {
      publishedInfo.setTitle(draftInfo.getTitle());
    }
    if (draftInfo.hasRelatedAssets()) {
      publishedInfo.setRelatedAssets(draftInfo.getRelatedAssets(), SetMode.IGNORE_NULL);
    }
    if (draftInfo.hasRelatedDocuments()) {
      publishedInfo.setRelatedDocuments(draftInfo.getRelatedDocuments(), SetMode.IGNORE_NULL);
    }
    if (draftInfo.hasParentDocument()) {
      publishedInfo.setParentDocument(draftInfo.getParentDocument(), SetMode.IGNORE_NULL);
    }

    // Update lastModified
    final AuditStamp now = new AuditStamp();
    now.setTime(System.currentTimeMillis());
    now.setActor(actorUrn);
    publishedInfo.setLastModified(now);

    // Ensure draftOf is NOT set on published document
    publishedInfo.setDraftOf(null, SetMode.REMOVE_IF_NULL);

    // Ingest updated published document
    final MetadataChangeProposal infoProposal = new MetadataChangeProposal();
    infoProposal.setEntityUrn(publishedUrn);
    infoProposal.setEntityType(Constants.DOCUMENT_ENTITY_NAME);
    infoProposal.setAspectName(Constants.DOCUMENT_INFO_ASPECT_NAME);
    infoProposal.setChangeType(ChangeType.UPSERT);
    infoProposal.setAspect(GenericRecordUtils.serializeAspect(publishedInfo));
    entityClient.ingestProposal(opContext, infoProposal, false);

    log.info("Merged draft {} into published document {}", draftUrn, publishedUrn);

    // Delete draft if requested
    if (deleteDraft) {
      deleteDocument(opContext, draftUrn);
      log.info("Deleted draft document {} after merge", draftUrn);
    }
  }

  /**
   * Get all draft documents for a published document.
   *
   * @param opContext the operation context
   * @param publishedDocumentUrn the URN of the published document
   * @param start starting offset
   * @param count number of results to return
   * @return SearchResult containing draft documents
   * @throws Exception if search fails
   */
  @Nonnull
  public SearchResult getDraftDocuments(
      @Nonnull OperationContext opContext, @Nonnull Urn publishedDocumentUrn, int start, int count)
      throws Exception {

    // Build filter for draftOf = publishedDocumentUrn
    final Filter filter = buildDraftOfFilter(publishedDocumentUrn);

    // Search for draft documents
    return entityClient.search(
        opContext.withSearchFlags(flags -> flags.setFulltext(false)),
        Constants.DOCUMENT_ENTITY_NAME,
        "*",
        filter,
        null, // sort criterion
        start,
        count);
  }

  /** Build a filter to find documents that are drafts of a specific document. */
  public static Filter buildDraftOfFilter(@Nonnull Urn draftOfUrn) {
    final Criterion criterion = new Criterion();
    criterion.setField("draftOf");
    criterion.setValue(draftOfUrn.toString());
    criterion.setCondition(Condition.EQUAL);

    final CriterionArray criterionArray = new CriterionArray();
    criterionArray.add(criterion);

    final ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(criterionArray);

    final ConjunctiveCriterionArray conjunctiveCriterionArray = new ConjunctiveCriterionArray();
    conjunctiveCriterionArray.add(conjunctiveCriterion);

    final Filter filter = new Filter();
    filter.setOr(conjunctiveCriterionArray);

    return filter;
  }
}
