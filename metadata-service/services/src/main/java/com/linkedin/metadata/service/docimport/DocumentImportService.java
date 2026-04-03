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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Service for importing documents from external sources (GitHub repositories, file uploads).
 * Creates or updates Document entities using deterministic IDs for idempotent re-imports.
 *
 * <p>Supports hierarchical imports: GitHub folder structure is preserved as parent-child document
 * relationships, and callers can specify a root parent document for the imported tree.
 *
 * <p>Authorization is NOT performed here — callers must check MANAGE_DOCUMENTS privilege.
 */
@Slf4j
public class DocumentImportService {

  private static final Pattern UNSAFE_CHARS = Pattern.compile("[^a-zA-Z0-9_.\\-]");
  private static final Pattern MULTI_DASH = Pattern.compile("-{2,}");
  private static final int MAX_ID_LENGTH = 200;
  private static final int MAX_TEXT_CHARS = 1_000_000;

  private final SystemEntityClient entityClient;
  private final GitHubDocumentSource gitHubSource;

  public DocumentImportService(@Nonnull SystemEntityClient entityClient) {
    this.entityClient = entityClient;
    this.gitHubSource = new GitHubDocumentSource();
  }

  // -- GitHub operations --

  @Nonnull
  public List<GitHubFileInfo> previewGitHubImport(
      @Nonnull String repoUrl,
      @Nonnull String branch,
      @Nonnull String pathPrefix,
      @Nonnull List<String> extensions,
      @Nonnull String githubToken) {

    String ownerRepo = GitHubDocumentSource.parseRepoIdentifier(repoUrl);
    String normalizedPath = normalizePath(pathPrefix);
    List<String> normalizedExts = normalizeExtensions(extensions);

    return gitHubSource.listFiles(ownerRepo, branch, normalizedPath, normalizedExts, githubToken);
  }

  /**
   * Import documents from a GitHub repository, preserving folder structure as parent-child
   * relationships. Folder documents are created for intermediate directories.
   *
   * @param parentDocumentUrn optional root parent — top-level imported items become children of
   *     this document. Null means import at root level.
   */
  @Nonnull
  public ImportResult importFromGitHub(
      @Nonnull OperationContext opContext,
      @Nonnull String repoUrl,
      @Nonnull String branch,
      @Nonnull String pathPrefix,
      @Nonnull List<String> extensions,
      @Nonnull String githubToken,
      @Nonnull ImportUseCase useCase,
      boolean showInGlobalContext,
      @Nullable Urn parentDocumentUrn,
      @Nonnull Urn actorUrn) {

    String ownerRepo = GitHubDocumentSource.parseRepoIdentifier(repoUrl);
    String normalizedPath = normalizePath(pathPrefix);
    List<String> normalizedExts = normalizeExtensions(extensions);

    List<DocumentCandidate> candidates =
        gitHubSource.fetchDocuments(ownerRepo, branch, normalizedPath, normalizedExts, githubToken);

    return importDocuments(
        opContext, candidates, useCase, showInGlobalContext, parentDocumentUrn, actorUrn);
  }

  // -- File upload operations --

  /**
   * Import documents from uploaded files. Each file becomes one Document entity.
   *
   * @param parentDocumentUrn optional parent — all uploaded files become children of this document.
   *     Null means import at root level.
   */
  @Nonnull
  public ImportResult importFromFiles(
      @Nonnull OperationContext opContext,
      @Nonnull List<Map.Entry<String, byte[]>> files,
      @Nonnull ImportUseCase useCase,
      boolean showInGlobalContext,
      @Nullable Urn parentDocumentUrn,
      @Nonnull Urn actorUrn) {

    List<DocumentCandidate> candidates = new ArrayList<>();
    for (Map.Entry<String, byte[]> file : files) {
      String filename = file.getKey();
      byte[] content = file.getValue();

      String text = TextExtractors.extract(content, filename);
      if (text == null) {
        log.warn("Skipping file {} — unsupported format or empty", filename);
        continue;
      }

      String ext = TextExtractors.getExtension(filename);
      Map<String, String> props = new HashMap<>();
      props.put("import_source", "file_upload");
      props.put("original_filename", filename);
      props.put("file_extension", ext);

      candidates.add(
          DocumentCandidate.builder()
              .title(TextExtractors.titleFromFilename(filename))
              .text(text)
              .sourceId(makeFileSourceId(filename))
              .customProperties(props)
              .build());
    }

    log.info("Parsed {} documents from {} uploaded files", candidates.size(), files.size());
    return importDocuments(
        opContext, candidates, useCase, showInGlobalContext, parentDocumentUrn, actorUrn);
  }

  @Nonnull
  public List<String> getSupportedFileExtensions() {
    return TextExtractors.getSupportedExtensions();
  }

  // -- Core import logic --

  /**
   * Import a batch of DocumentCandidates, creating or updating Document entities. Handles
   * parent-child wiring:
   *
   * <ul>
   *   <li>Candidates with {@code parentSourceId} are linked to the document created for that source
   *       ID
   *   <li>Candidates without {@code parentSourceId} (top-level) are linked to {@code rootParentUrn}
   *   <li>If {@code rootParentUrn} is null, top-level candidates have no parent
   * </ul>
   *
   * <p>Candidates MUST be ordered so that parent folders appear before their children (GitHub
   * source already returns them in this order).
   */
  @Nonnull
  public ImportResult importDocuments(
      @Nonnull OperationContext opContext,
      @Nonnull List<DocumentCandidate> candidates,
      @Nonnull ImportUseCase useCase,
      boolean showInGlobalContext,
      @Nullable Urn rootParentUrn,
      @Nonnull Urn actorUrn) {

    ImportResult result = new ImportResult();
    // Track sourceId → created URN so children can find their parent
    Map<String, Urn> sourceIdToUrn = new HashMap<>();

    for (DocumentCandidate candidate : candidates) {
      try {
        Urn parentUrn = resolveParentUrn(candidate, sourceIdToUrn, rootParentUrn);
        Urn createdUrn =
            importOneDocument(
                opContext, candidate, useCase, showInGlobalContext, parentUrn, actorUrn, result);
        sourceIdToUrn.put(candidate.getSourceId(), createdUrn);
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

  /**
   * Determine the parent URN for a candidate: if it has a parentSourceId, look up the URN from the
   * already-created docs; otherwise fall back to the root parent.
   */
  @Nullable
  private Urn resolveParentUrn(
      DocumentCandidate candidate, Map<String, Urn> sourceIdToUrn, @Nullable Urn rootParentUrn) {
    if (candidate.getParentSourceId() != null) {
      Urn resolved = sourceIdToUrn.get(candidate.getParentSourceId());
      if (resolved != null) {
        return resolved;
      }
      log.warn(
          "Parent source ID '{}' not found for candidate '{}', falling back to root parent",
          candidate.getParentSourceId(),
          candidate.getSourceId());
    }
    return rootParentUrn;
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
  static String makeDocumentId(@Nonnull String sourceId) {
    String safe = UNSAFE_CHARS.matcher(sourceId).replaceAll("-");
    safe = MULTI_DASH.matcher(safe).replaceAll("-");
    safe = safe.replaceAll("^-+|-+$", "").toLowerCase();
    if (safe.length() > MAX_ID_LENGTH) {
      safe = safe.substring(0, MAX_ID_LENGTH);
    }
    return safe;
  }

  @Nonnull
  static String makeFileSourceId(@Nonnull String filename) {
    String basename = filename;
    int slash = basename.lastIndexOf('/');
    if (slash >= 0) basename = basename.substring(slash + 1);
    int dot = basename.lastIndexOf('.');
    if (dot > 0) basename = basename.substring(0, dot);
    return "upload." + basename;
  }

  @Nonnull
  private static String normalizePath(@Nullable String path) {
    if (path == null || path.isBlank() || "/".equals(path.trim())) {
      return "";
    }
    return path.trim().replaceAll("^/+|/+$", "");
  }

  @Nonnull
  private static List<String> normalizeExtensions(@Nullable List<String> extensions) {
    if (extensions == null || extensions.isEmpty()) {
      return List.of(".md", ".txt");
    }
    return extensions.stream()
        .map(ext -> ext.startsWith(".") ? ext : "." + ext)
        .collect(Collectors.toList());
  }
}
