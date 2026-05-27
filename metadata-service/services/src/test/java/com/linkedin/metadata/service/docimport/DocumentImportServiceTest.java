package com.linkedin.metadata.service.docimport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.knowledge.DocumentInfo;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class DocumentImportServiceTest {

  private static final Urn ACTOR = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final Urn PARENT_DOC = UrnUtils.getUrn("urn:li:document:existing-parent");
  private static final OperationContext OP_CONTEXT =
      TestOperationContexts.userContextNoSearchAuthorization(ACTOR);

  @Test
  public void testMakeDocumentId_basicSanitization() {
    String id = DocumentImportService.makeDocumentId("upload.readme");
    assertTrue(id.startsWith("upload.readme-"));
    assertEquals(id.length(), "upload.readme-".length() + 8);
  }

  @Test
  public void testMakeDocumentId_specialCharsReplaced() {
    String id = DocumentImportService.makeDocumentId("upload.My File (copy)");
    assertTrue(id.startsWith("upload.my-file-copy-"));
  }

  @Test
  public void testMakeDocumentId_distinctAfterSanitization() {
    String id1 = DocumentImportService.makeDocumentId("upload.docs/a b");
    String id2 = DocumentImportService.makeDocumentId("upload.docs/a-b");
    assertNotEquals(id1, id2);
  }

  @Test
  public void testMakeFileSourceId() {
    assertEquals(DocumentImportService.makeFileSourceId("report.pdf"), "upload.report");
  }

  @Test
  public void testMakeFileSourceId_withPath() {
    assertEquals(DocumentImportService.makeFileSourceId("/tmp/uploads/doc.txt"), "upload.doc");
  }

  @Test
  public void testImportDocuments_flatCandidatesNoParent() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    DocumentImportService service = new DocumentImportService(mockClient);

    List<DocumentCandidate> candidates =
        List.of(
            DocumentCandidate.builder()
                .title("Doc A")
                .text("Content A")
                .sourceId("upload.docA")
                .build(),
            DocumentCandidate.builder()
                .title("Doc B")
                .text("Content B")
                .sourceId("upload.docB")
                .build());

    ImportResult result =
        service.importDocuments(
            OP_CONTEXT, candidates, ImportUseCase.CONTEXT_DOCUMENT, true, null, ACTOR);

    assertEquals(result.getCreatedCount(), 2);
    assertEquals(result.getUpdatedCount(), 0);
    assertEquals(result.getFailedCount(), 0);
    assertEquals(result.getDocumentUrns().size(), 2);
  }

  @Test
  public void testImportDocuments_flatCandidatesWithRootParent() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    DocumentImportService service = new DocumentImportService(mockClient);

    List<DocumentCandidate> candidates =
        List.of(
            DocumentCandidate.builder()
                .title("Child Doc")
                .text("Content")
                .sourceId("upload.child")
                .build());

    ImportResult result =
        service.importDocuments(
            OP_CONTEXT, candidates, ImportUseCase.CONTEXT_DOCUMENT, true, PARENT_DOC, ACTOR);

    assertEquals(result.getCreatedCount(), 1);

    // Verify the MCP batch includes a DocumentInfo with parentDocument set
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<MetadataChangeProposal>> mcpCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockClient, times(1))
        .batchIngestProposals(any(OperationContext.class), mcpCaptor.capture(), anyBoolean());

    List<MetadataChangeProposal> mcps = mcpCaptor.getValue();
    // CONTEXT_DOCUMENT has no subtype, so 2 MCPs: DocumentInfo + DocumentSettings
    assertEquals(mcps.size(), 2);

    MetadataChangeProposal infoMcp = mcps.get(0);
    assertEquals(infoMcp.getAspectName(), "documentInfo");

    DocumentInfo docInfo =
        com.linkedin.metadata.utils.GenericRecordUtils.deserializeAspect(
            infoMcp.getAspect().getValue(),
            infoMcp.getAspect().getContentType(),
            DocumentInfo.class);

    assertTrue(docInfo.hasParentDocument());
    assertEquals(docInfo.getParentDocument().getDocument(), PARENT_DOC);
  }

  @Test
  public void testImportDocuments_updatesExistingDocument() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(true);

    DocumentImportService service = new DocumentImportService(mockClient);

    ImportResult result =
        service.importDocuments(
            OP_CONTEXT,
            List.of(
                DocumentCandidate.builder()
                    .title("Existing")
                    .text("Updated")
                    .sourceId("upload.existing")
                    .build()),
            ImportUseCase.CONTEXT_DOCUMENT,
            true,
            null,
            ACTOR);

    assertEquals(result.getCreatedCount(), 0);
    assertEquals(result.getUpdatedCount(), 1);
  }

  @Test
  public void testImportDocuments_truncatesLongText() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    DocumentImportService service = new DocumentImportService(mockClient);

    // Create text that exceeds the 1M character limit
    String longText = "x".repeat(1_100_000);

    ImportResult result =
        service.importDocuments(
            OP_CONTEXT,
            List.of(
                DocumentCandidate.builder()
                    .title("Long Doc")
                    .text(longText)
                    .sourceId("upload.longdoc")
                    .build()),
            ImportUseCase.CONTEXT_DOCUMENT,
            true,
            null,
            ACTOR);

    assertEquals(result.getCreatedCount(), 1);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<MetadataChangeProposal>> mcpCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockClient)
        .batchIngestProposals(any(OperationContext.class), mcpCaptor.capture(), anyBoolean());

    DocumentInfo docInfo = extractDocumentInfo(mcpCaptor.getValue());
    String storedText = docInfo.getContents().getText();

    // Should be truncated to ~1M chars + the truncation notice
    assertTrue(storedText.length() < longText.length(), "Text should be truncated");
    assertTrue(storedText.endsWith("... (truncated)"), "Should end with truncation marker");
    assertTrue(storedText.length() <= 1_000_020, "Should be around 1M chars");
  }

  @Test
  public void testImportDocuments_shortTextNotTruncated() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    DocumentImportService service = new DocumentImportService(mockClient);

    String shortText = "Short content that should not be truncated";

    ImportResult result =
        service.importDocuments(
            OP_CONTEXT,
            List.of(
                DocumentCandidate.builder()
                    .title("Short Doc")
                    .text(shortText)
                    .sourceId("upload.shortdoc")
                    .build()),
            ImportUseCase.CONTEXT_DOCUMENT,
            true,
            null,
            ACTOR);

    assertEquals(result.getCreatedCount(), 1);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<MetadataChangeProposal>> mcpCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockClient)
        .batchIngestProposals(any(OperationContext.class), mcpCaptor.capture(), anyBoolean());

    DocumentInfo docInfo = extractDocumentInfo(mcpCaptor.getValue());
    assertEquals(docInfo.getContents().getText(), shortText);
  }

  @Test
  public void testImportDocuments_skillUseCaseSetsSubType() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    DocumentImportService service = new DocumentImportService(mockClient);

    ImportResult result =
        service.importDocuments(
            OP_CONTEXT,
            List.of(
                DocumentCandidate.builder()
                    .title("Skill Doc")
                    .text("Skill content")
                    .sourceId("upload.skill")
                    .build()),
            ImportUseCase.SKILL,
            true,
            null,
            ACTOR);

    assertEquals(result.getCreatedCount(), 1);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<MetadataChangeProposal>> mcpCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockClient)
        .batchIngestProposals(any(OperationContext.class), mcpCaptor.capture(), anyBoolean());

    // SKILL use case should produce 3 MCPs: DocumentInfo + SubTypes + DocumentSettings
    List<MetadataChangeProposal> mcps = mcpCaptor.getValue();
    assertEquals(mcps.size(), 3);
    assertTrue(
        mcps.stream().anyMatch(m -> "subTypes".equals(m.getAspectName())),
        "Should include subTypes MCP for SKILL use case");
  }

  @Test
  public void testImportDocuments_setsSourceTypeToNative() throws Exception {
    SystemEntityClient mockClient = mock(SystemEntityClient.class);
    when(mockClient.exists(any(OperationContext.class), any(Urn.class))).thenReturn(false);

    DocumentImportService service = new DocumentImportService(mockClient);

    service.importDocuments(
        OP_CONTEXT,
        List.of(
            DocumentCandidate.builder()
                .title("Native Doc")
                .text("Content")
                .sourceId("upload.nativedoc")
                .build()),
        ImportUseCase.CONTEXT_DOCUMENT,
        true,
        null,
        ACTOR);

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<MetadataChangeProposal>> mcpCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockClient)
        .batchIngestProposals(any(OperationContext.class), mcpCaptor.capture(), anyBoolean());

    DocumentInfo docInfo = extractDocumentInfo(mcpCaptor.getValue());
    assertTrue(docInfo.hasSource());
    assertEquals(
        docInfo.getSource().getSourceType().toString(),
        "NATIVE",
        "Source type should always be NATIVE");
  }

  @Test
  public void testMakeDocumentId_truncatesLongIds() {
    String longSource = "upload.really-long-name." + "x".repeat(300);
    String id = DocumentImportService.makeDocumentId(longSource);
    assertTrue(id.length() <= 200, "ID should be capped at 200 chars");
  }

  @Test
  public void testMakeDocumentId_lowercased() {
    String id = DocumentImportService.makeDocumentId("upload.MyDocument");
    assertTrue(id.startsWith("upload.mydocument-"));
  }

  @Test
  public void testMakeFileSourceId_noExtension() {
    assertEquals(DocumentImportService.makeFileSourceId("Makefile"), "upload.Makefile");
  }

  private static DocumentInfo extractDocumentInfo(List<MetadataChangeProposal> mcps) {
    MetadataChangeProposal infoMcp =
        mcps.stream()
            .filter(m -> "documentInfo".equals(m.getAspectName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("No documentInfo MCP found"));
    return com.linkedin.metadata.utils.GenericRecordUtils.deserializeAspect(
        infoMcp.getAspect().getValue(), infoMcp.getAspect().getContentType(), DocumentInfo.class);
  }
}
