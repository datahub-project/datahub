package com.linkedin.metadata.service.docimport;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.knowledge.DocumentContents;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.knowledge.DocumentSettings;
import com.linkedin.knowledge.DocumentSource;
import com.linkedin.knowledge.DocumentSourceType;
import com.linkedin.knowledge.DocumentState;
import com.linkedin.knowledge.DocumentStatus;
import com.linkedin.knowledge.ParentDocument;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for importing documents from file uploads initiated in the UI. Creates or updates
 * Document entities using deterministic IDs for idempotent re-imports.
 *
 * <p>Authorization is NOT performed here — callers must check MANAGE_DOCUMENTS privilege.
 */
@Slf4j
public class DocumentImportService {

  private static final Pattern UNSAFE_CHARS = Pattern.compile("[^a-zA-Z0-9_.\\-]");
  private static final Pattern MULTI_DASH = Pattern.compile("-{2,}");
  private static final int MAX_ID_LENGTH = 200;
  private static final int MAX_TEXT_CHARS = 1_000_000;

  /** Hex characters appended to document ids (8 bytes of SHA-256). */
  private static final int ID_HASH_SUFFIX_HEX_LENGTH = 16;

  private final SystemEntityClient entityClient;

  public DocumentImportService(@Nonnull SystemEntityClient entityClient) {
    this.entityClient = entityClient;
  }

  // -- Core import logic --

  /** Import file-upload candidates, optionally nesting each under {@code parentDocumentUrn}. */
  @Nonnull
  public ImportResult importDocuments(
      @Nonnull OperationContext opContext,
      @Nonnull List<DocumentCandidate> candidates,
      @Nonnull ImportUseCase useCase,
      boolean showInGlobalContext,
      @Nullable Urn parentDocumentUrn,
      @Nonnull Urn actorUrn) {

    ImportResult result = new ImportResult();

    if (parentDocumentUrn != null) {
      validateParentDocumentUrn(opContext, parentDocumentUrn);
    }

    for (DocumentCandidate candidate : candidates) {
      try {
        importOneDocument(
            opContext,
            candidate,
            useCase,
            showInGlobalContext,
            parentDocumentUrn,
            actorUrn,
            result);
      } catch (Exception e) {
        log.warn("Failed to import document '{}': {}", candidate.getSourceId(), e.getMessage());
        result.recordFailure(candidate.getSourceId(), e.getMessage());
      }
    }

    log.info(
        "Import complete: {} created, {} updated, {} failed",
        result.getCreatedCount(),
        result.getUpdatedCount(),
        result.getFailedCount());
    return result;
  }

  private Urn importOneDocument(
      OperationContext opContext,
      DocumentCandidate candidate,
      ImportUseCase useCase,
      boolean showInGlobalContext,
      @Nullable Urn parentDocumentUrn,
      Urn actorUrn,
      ImportResult result)
      throws Exception {

    String docId = makeDocumentId(candidate.getSourceId());
    Urn documentUrn =
        Urn.createFromString(String.format("urn:li:%s:%s", Constants.DOCUMENT_ENTITY_NAME, docId));

    boolean isUpdate = entityClient.exists(opContext, documentUrn);

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(System.currentTimeMillis());
    auditStamp.setActor(actorUrn);

    DocumentInfo documentInfo = new DocumentInfo();
    documentInfo.setTitle(candidate.getTitle(), SetMode.IGNORE_NULL);

    DocumentContents contents = new DocumentContents();
    String text = candidate.getText();
    if (text.length() > MAX_TEXT_CHARS) {
      log.warn(
          "Truncating document '{}' from {} to {} characters",
          candidate.getSourceId(),
          text.length(),
          MAX_TEXT_CHARS);
      text = text.substring(0, MAX_TEXT_CHARS) + "\n\n... (truncated)";
    }
    contents.setText(text);
    documentInfo.setContents(contents);

    DocumentSource source = new DocumentSource();
    source.setSourceType(DocumentSourceType.NATIVE);
    documentInfo.setSource(source);

    documentInfo.setCreated(auditStamp);
    documentInfo.setLastModified(auditStamp);

    DocumentStatus status = new DocumentStatus();
    status.setState(DocumentState.PUBLISHED);
    documentInfo.setStatus(status, SetMode.IGNORE_NULL);

    // Set parent document for hierarchy
    if (parentDocumentUrn != null) {
      ParentDocument parent = new ParentDocument();
      parent.setDocument(parentDocumentUrn);
      documentInfo.setParentDocument(parent, SetMode.IGNORE_NULL);
    }

    Map<String, String> allProps = new HashMap<>();
    allProps.put("import_source_id", candidate.getSourceId());
    allProps.putAll(candidate.getCustomProperties());
    documentInfo.setCustomProperties(new StringMap(allProps));

    List<MetadataChangeProposal> mcps = new ArrayList<>();

    MetadataChangeProposal infoMcp =
        AspectUtils.buildSynchronousMetadataChangeProposal(
            documentUrn, Constants.DOCUMENT_INFO_ASPECT_NAME, documentInfo);
    mcps.add(infoMcp);

    String subType = useCase.toSubType();
    if (subType != null) {
      SubTypes subTypes = new SubTypes();
      subTypes.setTypeNames(new StringArray(Collections.singletonList(subType)));
      MetadataChangeProposal subTypesMcp =
          AspectUtils.buildSynchronousMetadataChangeProposal(
              documentUrn, Constants.SUB_TYPES_ASPECT_NAME, subTypes);
      mcps.add(subTypesMcp);
    }

    DocumentSettings settings = new DocumentSettings();
    settings.setShowInGlobalContext(showInGlobalContext);
    settings.setLastModified(auditStamp, SetMode.IGNORE_NULL);
    MetadataChangeProposal settingsMcp =
        AspectUtils.buildSynchronousMetadataChangeProposal(
            documentUrn, Constants.DOCUMENT_SETTINGS_ASPECT_NAME, settings);
    mcps.add(settingsMcp);

    entityClient.batchIngestProposals(opContext, mcps, false);

    result.recordSuccess(documentUrn.toString(), isUpdate);
    log.debug(
        "{} document {} from source '{}' (parent: {})",
        isUpdate ? "Updated" : "Created",
        documentUrn,
        candidate.getSourceId(),
        parentDocumentUrn);
    return documentUrn;
  }

  // -- ID generation --

  @Nonnull
  public static String makeDocumentId(@Nonnull String sourceId) {
    String hashSuffix = hashSourceIdSuffix(sourceId);
    String safe = UNSAFE_CHARS.matcher(sourceId).replaceAll("-");
    safe = MULTI_DASH.matcher(safe).replaceAll("-");
    safe = safe.replaceAll("^-+|-+$", "").toLowerCase();
    int maxBaseLength = MAX_ID_LENGTH - hashSuffix.length() - 1;
    if (maxBaseLength < 1) {
      return hashSuffix;
    }
    if (safe.length() > maxBaseLength) {
      safe = safe.substring(0, maxBaseLength);
    }
    return safe + "-" + hashSuffix;
  }

  @Nonnull
  private static String hashSourceIdSuffix(@Nonnull String sourceId) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(sourceId.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash, 0, ID_HASH_SUFFIX_HEX_LENGTH / 2);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private void validateParentDocumentUrn(
      @Nonnull OperationContext opContext, @Nonnull Urn parentDocumentUrn) {
    if (!Constants.DOCUMENT_ENTITY_NAME.equals(parentDocumentUrn.getEntityType())) {
      throw new IllegalArgumentException(
          String.format(
              "parentDocumentUrn must be a document entity, got: %s",
              parentDocumentUrn.getEntityType()));
    }
    try {
      if (!entityClient.exists(opContext, parentDocumentUrn)) {
        throw new IllegalArgumentException(
            String.format("Parent document does not exist: %s", parentDocumentUrn));
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format("Failed to validate parent document %s", parentDocumentUrn), e);
    }
  }

  @Nonnull
  public static String makeFileSourceId(@Nonnull String filename) {
    String basename = filename;
    int slash = basename.lastIndexOf('/');
    if (slash >= 0) basename = basename.substring(slash + 1);
    int dot = basename.lastIndexOf('.');
    if (dot > 0) basename = basename.substring(0, dot);
    return "upload." + basename;
  }
}
