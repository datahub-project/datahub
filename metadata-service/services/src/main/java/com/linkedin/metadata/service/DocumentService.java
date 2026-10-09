package com.linkedin.metadata.service;

import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.SemanticText;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
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
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.DocumentKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 * <p>Authorization is enforced on public mutating methods and single-document reads via {@link
 * DocumentAuthorizationUtils}. {@link #searchDocuments} returns only documents the actor can view;
 * search totals/facets may still reflect the broader index hit set. System authentication
 * short-circuits those checks so bridge and other system writers continue to work.
 */
@Slf4j
public class DocumentService {

  private static final String WILDCARD_QUERY = "*";
  private static final String PARENT_DOCUMENT_FIELD = "parentDocument";

  private final SystemEntityClient entityClient;

  public DocumentService(@Nonnull SystemEntityClient entityClient) {
    this.entityClient = entityClient;
  }

  /**
   * Builds an UPSERT proposal honoring the caller's {@link SearchIndexMode}. SYNC proposals are
   * marked so GMS updates the indices in the request path and the MAE consumer skips them; ASYNC
   * proposals are indexed by the consumer. Every proposal a mutation emits must use the same mode —
   * splitting one document's writes across the two index writers is exactly the out-of-order stale
   * state {@link SearchIndexMode} warns about.
   */
  @Nonnull
  private static MetadataChangeProposal buildProposal(
      @Nonnull Urn urn,
      @Nonnull String aspectName,
      @Nonnull com.linkedin.data.template.RecordTemplate aspect,
      @Nonnull SearchIndexMode indexMode) {
    // Fail fast rather than silently routing a null mode down the ASYNC path — a caller that
    // didn't choose a mode must not accidentally split the document across two index writers.
    Objects.requireNonNull(indexMode, "indexMode is required");
    if (indexMode == SearchIndexMode.SYNC) {
      return AspectUtils.buildSynchronousMetadataChangeProposal(urn, aspectName, aspect);
    }
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    return proposal;
  }

  /**
   * Creates a new document.
   *
   * @param opContext the operation context
   * @param id optional custom ID (if null, generates a UUID)
   * @param subTypes optional list of document sub-types
   * @param title optional title
   * @param source optional source information for externally ingested documents
   * @param state optional initial state (UNPUBLISHED or PUBLISHED)
   * @param text the document text text
   * @param parentDocumentUrn optional parent document URN
   * @param relatedAssetUrns optional list of related asset URNs
   * @param relatedDocumentUrns optional list of related document URNs
   * @param settings optional document settings (defaults to showInGlobalContext=true if not
   *     provided)
   * @param actorUrn the URN of the user creating the document
   * @param indexMode how the write is applied to the search index (one mode per document)
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
      @Nullable com.linkedin.knowledge.DocumentSettings settings,
      @Nonnull Urn actorUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {
    return createDocument(
        opContext,
        id,
        subTypes,
        title,
        source,
        state,
        text,
        parentDocumentUrn,
        relatedAssetUrns,
        relatedDocumentUrns,
        settings,
        null,
        actorUrn,
        indexMode);
  }

  /**
   * Creates a new document with its initial ownership in the same ingest batch.
   *
   * @param owners optional initial owners; defaults to the creator
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
      @Nullable com.linkedin.knowledge.DocumentSettings settings,
      @Nullable List<Owner> owners,
      @Nonnull Urn actorUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {

    // Generate document URN
    final String documentId = id != null ? id : UUID.randomUUID().toString();
    final Urn documentUrn =
        Urn.createFromString(
            String.format("urn:li:%s:%s", Constants.DOCUMENT_ENTITY_NAME, documentId));

    DocumentAuthorizationUtils.assertCanCreate(opContext, documentUrn);

    // Check if document already exists
    if (entityClient.exists(opContext, documentUrn)) {
      throw new IllegalArgumentException(
          String.format("Document with ID %s already exists", documentId));
    }

    if (parentDocumentUrn != null) {
      if (documentUrn.equals(parentDocumentUrn)) {
        throw new IllegalArgumentException("Cannot create a Document with itself as parent");
      }
      assertParentIsLive(opContext, parentDocumentUrn);
    }

    // Draft feature is not yet implemented in the UI

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

    // Set status (default to UNPUBLISHED if not provided)
    final com.linkedin.knowledge.DocumentStatus status =
        new com.linkedin.knowledge.DocumentStatus();
    com.linkedin.knowledge.DocumentState finalState =
        state != null ? state : com.linkedin.knowledge.DocumentState.UNPUBLISHED;
    status.setState(finalState);
    documentInfo.setStatus(status, SetMode.IGNORE_NULL);

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
    final MetadataChangeProposal infoMcp =
        buildProposal(documentUrn, Constants.DOCUMENT_INFO_ASPECT_NAME, documentInfo, indexMode);

    // Prepare list of MCPs to ingest
    final List<MetadataChangeProposal> mcps = new java.util.ArrayList<>();
    mcps.add(infoMcp);

    // Create synchronous MCP for subTypes if provided
    if (subTypes != null && !subTypes.isEmpty()) {
      final com.linkedin.common.SubTypes subTypesAspect = new com.linkedin.common.SubTypes();
      subTypesAspect.setTypeNames(new com.linkedin.data.template.StringArray(subTypes));

      final MetadataChangeProposal subTypesMcp =
          buildProposal(documentUrn, Constants.SUB_TYPES_ASPECT_NAME, subTypesAspect, indexMode);
      mcps.add(subTypesMcp);
    }

    // Create synchronous MCP for document settings (defaults to showInGlobalContext=true)
    final com.linkedin.knowledge.DocumentSettings finalSettings =
        settings != null ? settings : new com.linkedin.knowledge.DocumentSettings();
    if (settings == null) {
      finalSettings.setShowInGlobalContext(true);
    }

    final AuditStamp settingsAuditStamp = new AuditStamp();
    settingsAuditStamp.setTime(System.currentTimeMillis());
    settingsAuditStamp.setActor(actorUrn);
    finalSettings.setLastModified(settingsAuditStamp, SetMode.IGNORE_NULL);

    final MetadataChangeProposal settingsMcp =
        buildProposal(
            documentUrn, Constants.DOCUMENT_SETTINGS_ASPECT_NAME, finalSettings, indexMode);
    mcps.add(settingsMcp);

    final List<Owner> initialOwners;
    if (owners != null && !owners.isEmpty()) {
      initialOwners = owners;
    } else {
      final Owner creatorOwner = new Owner();
      creatorOwner.setOwner(actorUrn);
      creatorOwner.setType(OwnershipType.TECHNICAL_OWNER);
      initialOwners = Collections.singletonList(creatorOwner);
    }
    final MetadataChangeProposal ownershipMcp =
        buildProposal(
            documentUrn,
            Constants.OWNERSHIP_ASPECT_NAME,
            buildOwnership(initialOwners, actorUrn),
            indexMode);
    mcps.add(ownershipMcp);

    // Ingest the document with all aspects
    entityClient.batchIngestProposals(opContext, mcps, false);

    log.debug("Created document {} for user {}", documentUrn, actorUrn);
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
    DocumentAuthorizationUtils.assertCanView(opContext, documentUrn);
    return getDocumentInfoWithoutAuthorization(opContext, documentUrn);
  }

  @Nullable
  private DocumentInfo getDocumentInfoWithoutAuthorization(
      @Nonnull OperationContext opContext, @Nonnull Urn documentUrn) throws Exception {

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
      @Nonnull Urn actorUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {
    updateDocumentContents(
        opContext, documentUrn, text, null, title, subTypes, actorUrn, indexMode);
  }

  /**
   * Updates the contents of a document, including an optional dedicated semantic-search payload.
   *
   * @param opContext the operation context
   * @param documentUrn the document URN
   * @param text the new display text
   * @param semanticText optional semantic-search content
   * @param title optional updated title
   * @param subTypes optional updated sub-types
   * @throws Exception if update fails
   */
  public void updateDocumentContents(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nullable String text,
      @Nullable String semanticText,
      @Nullable String title,
      @Nullable List<String> subTypes,
      @Nonnull Urn actorUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {

    DocumentAuthorizationUtils.assertCanUpdate(opContext, documentUrn);

    // Get existing info
    final DocumentInfo existingInfo = getDocumentInfoWithoutAuthorization(opContext, documentUrn);
    if (existingInfo == null) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Update text if provided
    if (text != null) {
      if (!existingInfo.hasContents()) {
        existingInfo.setContents(new DocumentContents());
      }
      existingInfo.getContents().setText(text);
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
    mcps.add(
        buildProposal(documentUrn, Constants.DOCUMENT_INFO_ASPECT_NAME, existingInfo, indexMode));

    // semanticText is a standalone aspect. Only write it when the caller opts in so ordinary
    // document body/title mutations leave an existing curated embedding source untouched.
    if (semanticText != null) {
      mcps.add(
          buildProposal(
              documentUrn,
              Constants.SEMANTIC_TEXT_ASPECT_NAME,
              new SemanticText().setText(semanticText),
              indexMode));
    }

    // Update subTypes if provided
    if (subTypes != null && !subTypes.isEmpty()) {
      final com.linkedin.common.SubTypes subTypesAspect = new com.linkedin.common.SubTypes();
      subTypesAspect.setTypeNames(new com.linkedin.data.template.StringArray(subTypes));
      mcps.add(
          buildProposal(documentUrn, Constants.SUB_TYPES_ASPECT_NAME, subTypesAspect, indexMode));
    }

    // Batch ingest all proposals
    entityClient.batchIngestProposals(opContext, mcps, false);

    log.debug("Updated contents for document {}", documentUrn);
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
      @Nonnull Urn actorUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {

    DocumentAuthorizationUtils.assertCanUpdate(opContext, documentUrn);

    // Fetch existing info
    final DocumentInfo info = getDocumentInfoWithoutAuthorization(opContext, documentUrn);
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
    final MetadataChangeProposal mcp =
        buildProposal(documentUrn, Constants.DOCUMENT_INFO_ASPECT_NAME, info, indexMode);

    entityClient.ingestProposal(opContext, mcp, false);

    log.debug("Updated related entities for document {}", documentUrn);
  }

  /**
   * Rejects a parent that was never stored or is soft-deleted. {@code includeSoftDelete=false}
   * treats both as absent.
   */
  private void assertParentIsLive(@Nonnull OperationContext opContext, @Nonnull Urn parentUrn)
      throws RemoteInvocationException {
    if (!entityClient.exists(opContext, parentUrn, false)) {
      throw new IllegalArgumentException(
          String.format("Parent Document with URN %s does not exist", parentUrn));
    }
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
      @Nonnull Urn actorUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {

    DocumentAuthorizationUtils.assertCanUpdate(opContext, documentUrn);

    // Verify document exists
    if (!entityClient.exists(opContext, documentUrn)) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // A missing or soft-deleted parent would make this document an orphan.
    if (newParentUrn != null) {
      assertParentIsLive(opContext, newParentUrn);

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
    final DocumentInfo info = getDocumentInfoWithoutAuthorization(opContext, documentUrn);
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
    final MetadataChangeProposal mcp =
        buildProposal(documentUrn, Constants.DOCUMENT_INFO_ASPECT_NAME, info, indexMode);

    entityClient.ingestProposal(opContext, mcp, false);

    log.debug("Moved document {} to parent {}", documentUrn, newParentUrn);
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
      @Nonnull Urn actorUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {

    DocumentAuthorizationUtils.assertCanUpdate(opContext, documentUrn);

    // Verify document exists
    if (!entityClient.exists(opContext, documentUrn)) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Fetch existing info
    final DocumentInfo info = getDocumentInfoWithoutAuthorization(opContext, documentUrn);
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
    final MetadataChangeProposal mcp =
        buildProposal(documentUrn, Constants.DOCUMENT_INFO_ASPECT_NAME, info, indexMode);

    entityClient.ingestProposal(opContext, mcp, false);

    log.debug("Updated status of document {} to {}", documentUrn, newState);
  }

  /**
   * Update the settings of a document.
   *
   * @param opContext the operation context
   * @param documentUrn the URN of the document to update
   * @param settings the new settings
   * @param actorUrn the URN of the user updating the settings
   * @throws Exception if update fails
   */
  public void updateDocumentSettings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nonnull com.linkedin.knowledge.DocumentSettings settings,
      @Nonnull Urn actorUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {

    DocumentAuthorizationUtils.assertCanUpdate(opContext, documentUrn);

    // Verify document exists
    if (!entityClient.exists(opContext, documentUrn)) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    // Set last modified
    final AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(System.currentTimeMillis());
    lastModified.setActor(actorUrn);
    settings.setLastModified(lastModified, SetMode.IGNORE_NULL);

    // Create metadata change proposal for DocumentSettings
    final MetadataChangeProposal settingsMcp =
        buildProposal(documentUrn, Constants.DOCUMENT_SETTINGS_ASPECT_NAME, settings, indexMode);

    // Also update lastModified timestamp in DocumentInfo
    final DocumentInfo info = getDocumentInfoWithoutAuthorization(opContext, documentUrn);
    if (info != null) {
      final AuditStamp infoLastModified = new AuditStamp();
      infoLastModified.setTime(System.currentTimeMillis());
      infoLastModified.setActor(actorUrn);
      info.setLastModified(infoLastModified);

      final MetadataChangeProposal infoMcp =
          buildProposal(documentUrn, Constants.DOCUMENT_INFO_ASPECT_NAME, info, indexMode);

      // Batch ingest both proposals
      entityClient.batchIngestProposals(
          opContext, java.util.Arrays.asList(settingsMcp, infoMcp), false);
    } else {
      // Just ingest settings if info doesn't exist (shouldn't happen)
      entityClient.ingestProposal(opContext, settingsMcp, false);
    }

    log.debug("Updated settings for document {}", documentUrn);
  }

  /**
   * Update the sub type for a document
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
      @Nonnull Urn actorUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {

    DocumentAuthorizationUtils.assertCanUpdate(opContext, documentUrn);

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
    final MetadataChangeProposal subTypesMcp =
        buildProposal(documentUrn, Constants.SUB_TYPES_ASPECT_NAME, subTypesAspect, indexMode);

    // Also update lastModified timestamp in DocumentInfo
    final DocumentInfo info = getDocumentInfoWithoutAuthorization(opContext, documentUrn);
    if (info != null) {
      final AuditStamp lastModified = new AuditStamp();
      lastModified.setTime(System.currentTimeMillis());
      lastModified.setActor(actorUrn);
      info.setLastModified(lastModified);

      final MetadataChangeProposal infoMcp =
          buildProposal(documentUrn, Constants.DOCUMENT_INFO_ASPECT_NAME, info, indexMode);

      // Batch ingest both proposals
      entityClient.batchIngestProposals(
          opContext, java.util.Arrays.asList(subTypesMcp, infoMcp), false);
    } else {
      // Just ingest subTypes if info doesn't exist (shouldn't happen)
      entityClient.ingestProposal(opContext, subTypesMcp, false);
    }

    log.debug("Updated sub-type for document {} to {}", documentUrn, subType);
  }

  /**
   * Soft deletes a document and every live document under it by setting {@code status.removed} to
   * true. Deleting a document deletes its live subtree.
   *
   * <p>Authorization is checked on the root only. Descendant lookup uses system search privileges
   * so drafts, unpublished documents, and documents hidden from global context are included. Status
   * writes still use {@code opContext}, so the audit stamp stays the acting user.
   *
   * <p>{@code exists} includes soft-deleted rows. A retry of an already-removed root is not "does
   * not exist"; the walk only sees documents that are still {@code removed=false}.
   *
   * @param opContext the operation context
   * @param documentUrn the document URN to soft delete
   * @return the deleted URNs, root included, deepest first
   * @throws DocumentDeleteLimitException when the live subtree exceeds {@link
   *     DocumentDeleteResult#MAX_DESCENDANTS} or {@link DocumentDeleteResult#MAX_DEPTH}; nothing is
   *     written
   * @throws Exception if deletion fails
   */
  @Nonnull
  public DocumentDeleteResult deleteDocument(
      @Nonnull OperationContext opContext,
      @Nonnull Urn documentUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {

    DocumentAuthorizationUtils.assertCanDelete(opContext, documentUrn);

    // Include soft-deleted rows. A missing key still throws; an already-removed root does not.
    if (!entityClient.exists(opContext, documentUrn)) {
      throw new IllegalArgumentException(
          String.format("Document with URN %s does not exist", documentUrn));
    }

    final OperationContext scrollContext = descendantScrollContext(opContext);
    final List<Urn> deleteOrder = collectDeleteOrder(scrollContext, documentUrn);

    final Map<Urn, Status> storedStatus = loadStoredStatus(scrollContext, deleteOrder);
    final List<MetadataChangeProposal> proposals = new ArrayList<>(deleteOrder.size());
    for (Urn urn : deleteOrder) {
      final Urn documentUrnToRemove = Objects.requireNonNull(urn);
      final Status existing = storedStatus.get(documentUrnToRemove);
      // Copy the stored aspect so lifecycleStage and lifecycleLastUpdated stay.
      final Status status = existing == null ? new Status() : new Status(existing.data().copy());
      status.setRemoved(true);
      proposals.add(
          buildProposal(documentUrnToRemove, Constants.STATUS_ASPECT_NAME, status, indexMode));
    }

    // One call. JavaEntityClient partitions at the ingest batch size, deepest first so a failed
    // later batch leaves the root live and a retry still finds the remaining removed=false nodes.
    entityClient.batchIngestProposals(opContext, proposals, false);
    final int descendantCount = deleteOrder.size() - 1;
    log.debug("Soft deleted document {} and {} nested documents", documentUrn, descendantCount);
    return new DocumentDeleteResult(deleteOrder, descendantCount);
  }

  /**
   * Search flags for a live document subtree. Removed documents stay excluded. Hidden lifecycle
   * stages stay included, and query rewrite stays off so {@code DocumentExpansionRewriter} cannot
   * truncate the walk. Drafts and documents that opt out of global context are already returned:
   * those filters are applied by GraphQL, not by this service-level scroll.
   */
  @Nonnull
  public static OperationContext withLiveSubtreeSearchFlags(@Nonnull OperationContext opContext) {
    return Objects.requireNonNull(
        opContext.withSearchFlags(
            flags ->
                flags
                    .setIncludeSoftDeleted(false)
                    .setIncludeHiddenLifecycleStages(true)
                    .setRewriteQuery(false)
                    .setSkipCache(true)
                    .setSkipHighlighting(true)
                    .setSkipAggregates(true)));
  }

  /**
   * Scroll context for descendant lookup. {@code isSystemAuth} is true only when the session actor
   * is the system actor, so a user session has to be rebuilt on the context's system
   * authentication. Search access control would otherwise drop documents the caller cannot read.
   */
  @Nonnull
  private OperationContext descendantScrollContext(@Nonnull OperationContext opContext) {
    if (opContext.isSystemAuth()) {
      return withLiveSubtreeSearchFlags(opContext);
    }
    final Authentication systemAuthentication =
        Objects.requireNonNull(
            opContext
                .getSystemAuthentication()
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "System authentication is required to collect documents for deletion")));
    final OperationContext systemContext =
        opContext.toBuilder()
            .operationContextConfig(
                Objects.requireNonNull(
                    opContext.getOperationContextConfig().toBuilder()
                        .allowSystemAuthentication(true)
                        .build()))
            .build(
                systemAuthentication,
                opContext.getSessionActorContext().isEnforceExistenceEnabled());
    return withLiveSubtreeSearchFlags(systemContext);
  }

  /**
   * Live descendants, deepest first, root last. A tree past either cap throws before any proposal
   * is built. Visited URNs are deleted once, so a cycle or a diamond does not write twice.
   */
  @Nonnull
  private List<Urn> collectDeleteOrder(@Nonnull OperationContext scrollContext, @Nonnull Urn root)
      throws Exception {
    final List<List<Urn>> levels = new ArrayList<>();
    levels.add(new ArrayList<>(List.of(root)));
    final Set<Urn> visited = new HashSet<>();
    visited.add(root);

    for (int depth = 0; ; depth++) {
      // A non-empty level only exists when the previous pass found a new child.
      if (depth > DocumentDeleteResult.MAX_DEPTH) {
        throw deleteLimit(
            root,
            Objects.requireNonNull(
                String.format("live subtree exceeds depth %s", DocumentDeleteResult.MAX_DEPTH)));
      }

      final List<Urn> nextLevel = new ArrayList<>();
      final List<Urn> level = levels.get(depth);
      for (int start = 0; start < level.size(); start += DocumentDeleteResult.SCROLL_PAGE_SIZE) {
        final int end = Math.min(start + DocumentDeleteResult.SCROLL_PAGE_SIZE, level.size());
        final List<Urn> parents = new ArrayList<>(level.subList(start, end));
        nextLevel.addAll(scrollChildDocuments(scrollContext, parents, root, visited));
      }
      if (nextLevel.isEmpty()) {
        break;
      }
      levels.add(nextLevel);
    }

    final List<Urn> deleteOrder = new ArrayList<>(visited.size());
    for (int depth = levels.size() - 1; depth >= 0; depth--) {
      deleteOrder.addAll(levels.get(depth));
    }
    return deleteOrder;
  }

  /**
   * Direct children of {@code parents}. One scroll covers the whole chunk. A hit is kept when its
   * stored {@code parentDocument} is one of {@code parents}: a stale index entry whose stored
   * parent left this wave is skipped, and a child stored under another parent in the same wave is
   * kept. Accepted children are added to {@code visited}. Another page is not requested once {@link
   * DocumentDeleteResult#MAX_DESCENDANTS} new documents are already accepted.
   */
  @Nonnull
  private List<Urn> scrollChildDocuments(
      @Nonnull OperationContext scrollContext,
      @Nonnull List<Urn> parents,
      @Nonnull Urn root,
      @Nonnull Set<Urn> visited)
      throws Exception {
    final Set<Urn> parentSet = new HashSet<>(parents);
    final List<Urn> children = new ArrayList<>();
    String scrollId = null;
    do {
      final ScrollResult page =
          entityClient.scrollAcrossEntities(
              scrollContext,
              Objects.requireNonNull(List.of(Constants.DOCUMENT_ENTITY_NAME)),
              WILDCARD_QUERY,
              buildParentDocumentsFilter(parents),
              scrollId,
              DocumentDeleteResult.SCROLL_KEEP_ALIVE,
              null,
              DocumentDeleteResult.SCROLL_PAGE_SIZE,
              List.of());
      if (page == null) {
        throw new IllegalStateException(
            String.format("Scroll for children of %s returned no result", parents));
      }
      final String nextScrollId = page.getScrollId();
      final List<SearchEntity> entities = page.getEntities();
      // An empty page that still carries a scroll id is a broken scroll, not the end of the tree.
      if (entities == null || entities.isEmpty()) {
        if (nextScrollId != null) {
          throw new IllegalStateException(
              String.format(
                  "Scroll for children of %s returned an empty page with scroll id %s",
                  parents, nextScrollId));
        }
        break;
      }
      final List<Urn> candidates = new ArrayList<>();
      for (SearchEntity entity : entities) {
        if (entity == null || entity.getEntity() == null) {
          continue;
        }
        candidates.add(Objects.requireNonNull(entity.getEntity()));
      }
      if (!candidates.isEmpty()) {
        final Map<Urn, EntityResponse> stored =
            Objects.requireNonNull(
                entityClient.batchGetV2(
                    scrollContext,
                    Constants.DOCUMENT_ENTITY_NAME,
                    Objects.requireNonNull(new HashSet<>(candidates)),
                    Set.of(Constants.DOCUMENT_INFO_ASPECT_NAME),
                    false));
        for (Urn candidate : candidates) {
          final Urn child = Objects.requireNonNull(candidate);
          final Urn storedParent = storedParent(stored.get(child));
          // Storage decides membership. A child stored under another parent in this wave stays;
          // one stored outside the wave was indexed here after it moved.
          if (storedParent == null || !parentSet.contains(storedParent)) {
            continue;
          }
          if (visited.contains(child)) {
            continue;
          }
          if (visited.size() - 1 >= DocumentDeleteResult.MAX_DESCENDANTS) {
            throw deleteLimit(
                root,
                Objects.requireNonNull(
                    String.format(
                        "live subtree exceeds %s descendants",
                        DocumentDeleteResult.MAX_DESCENDANTS)));
          }
          visited.add(child);
          children.add(child);
        }
      }
      scrollId = nextScrollId;
    } while (scrollId != null);
    return children;
  }

  @Nullable
  private static Urn storedParent(@Nullable EntityResponse response) {
    if (response == null || response.getAspects() == null) {
      return null;
    }
    final EnvelopedAspect aspect = response.getAspects().get(Constants.DOCUMENT_INFO_ASPECT_NAME);
    if (aspect == null || aspect.getValue() == null) {
      return null;
    }
    final DocumentInfo info = new DocumentInfo(aspect.getValue().data());
    final ParentDocument parent = info.getParentDocument();
    if (parent == null || !parent.hasDocument()) {
      return null;
    }
    return parent.getDocument();
  }

  @Nonnull
  private Map<Urn, Status> loadStoredStatus(
      @Nonnull OperationContext scrollContext, @Nonnull List<Urn> urns) throws Exception {
    final Map<Urn, Status> stored = new HashMap<>();
    for (int start = 0; start < urns.size(); start += DocumentDeleteResult.SCROLL_PAGE_SIZE) {
      final int end = Math.min(start + DocumentDeleteResult.SCROLL_PAGE_SIZE, urns.size());
      final Map<Urn, EntityResponse> page =
          Objects.requireNonNull(
              entityClient.batchGetV2(
                  scrollContext,
                  Constants.DOCUMENT_ENTITY_NAME,
                  Objects.requireNonNull(new HashSet<>(urns.subList(start, end))),
                  Set.of(Constants.STATUS_ASPECT_NAME),
                  false));
      for (Map.Entry<Urn, EntityResponse> entry : page.entrySet()) {
        final Urn urn = entry.getKey();
        final Status status = statusFrom(entry.getValue());
        if (urn != null && status != null) {
          stored.put(urn, status);
        }
      }
    }
    return stored;
  }

  @Nullable
  private static Status statusFrom(@Nullable EntityResponse response) {
    if (response == null || response.getAspects() == null) {
      return null;
    }
    final EnvelopedAspect aspect = response.getAspects().get(Constants.STATUS_ASPECT_NAME);
    if (aspect == null || aspect.getValue() == null) {
      return null;
    }
    return new Status(aspect.getValue().data());
  }

  @Nonnull
  private DocumentDeleteLimitException deleteLimit(@Nonnull Urn rootUrn, @Nonnull String reason) {
    log.warn("Refusing to delete document {}: {}", rootUrn, reason);
    return new DocumentDeleteLimitException(rootUrn, reason);
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
      @Nonnull List<Owner> owners,
      @Nonnull Urn actorUrn,
      @Nonnull SearchIndexMode indexMode)
      throws Exception {

    DocumentAuthorizationUtils.assertCanUpdate(opContext, documentUrn);

    // Create MCP for ownership
    final MetadataChangeProposal mcp =
        buildProposal(
            documentUrn,
            Constants.OWNERSHIP_ASPECT_NAME,
            buildOwnership(owners, actorUrn),
            indexMode);

    entityClient.ingestProposal(opContext, mcp, false);

    log.debug("Set ownership for document {} with {} owners", documentUrn, owners.size());
  }

  @Nonnull
  private static Ownership buildOwnership(@Nonnull List<Owner> owners, @Nonnull Urn actorUrn) {
    final Ownership ownership = new Ownership();
    final OwnerArray ownerArray = new OwnerArray();
    ownerArray.addAll(owners);
    ownership.setOwners(ownerArray);

    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(actorUrn);
    ownership.setLastModified(auditStamp);
    return ownership;
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

    SearchResult result =
        entityClient.search(
            opContext.withSearchFlags(flags -> flags.setFulltext(true)),
            Constants.DOCUMENT_ENTITY_NAME,
            query,
            filter,
            Collections.singletonList(sort),
            start,
            count);
    // Filter unauthorized hits rather than failing the whole page: GraphQL search callers expect
    // mixed-access result sets, and bridge inheritance can authorize only a subset of documents.
    result.setEntities(
        new com.linkedin.metadata.search.SearchEntityArray(
            result.getEntities().stream()
                .filter(
                    entity ->
                        DocumentAuthorizationUtils.isAuthorizedDocumentOperation(
                            opContext, READ, entity.getEntity()))
                .toList()));
    return result;
  }

  @Nonnull
  private static Filter buildParentDocumentsFilter(@Nonnull List<Urn> parents) {
    final StringArray values = new StringArray();
    for (Urn parent : parents) {
      values.add(Objects.requireNonNull(parent).toString());
    }
    final Criterion parentCriterion =
        new Criterion()
            .setField(PARENT_DOCUMENT_FIELD)
            .setValues(values)
            .setCondition(Condition.EQUAL);
    return Objects.requireNonNull(
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(new CriterionArray(Collections.singletonList(parentCriterion))))));
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
            .setField(PARENT_DOCUMENT_FIELD)
            .setValues(new StringArray(Collections.singletonList(parentDocumentUrn.toString())))
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
      DocumentInfo parentInfo = getDocumentInfoWithoutAuthorization(opContext, currentParent);
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
}
