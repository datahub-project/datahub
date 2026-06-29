package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DocumentFileInput;
import com.linkedin.datahub.graphql.generated.DocumentImportUseCase;
import com.linkedin.datahub.graphql.generated.ImportDocumentsFromFilesInput;
import com.linkedin.datahub.graphql.generated.ImportDocumentsResult;
import com.linkedin.metadata.service.docimport.DocumentCandidate;
import com.linkedin.metadata.service.docimport.DocumentImportService;
import com.linkedin.metadata.service.docimport.ImportResult;
import com.linkedin.metadata.service.docimport.ImportUseCase;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ImportDocumentsFromFilesResolverTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final String PARENT_DOCUMENT_URN = "urn:li:document:parent-doc";

  private DocumentImportService mockImportService;
  private ImportDocumentsFromFilesResolver resolver;
  private DataFetchingEnvironment mockEnv;

  @BeforeMethod
  public void setUp() {
    mockImportService = mock(DocumentImportService.class);
    mockEnv = mock(DataFetchingEnvironment.class);
    resolver = new ImportDocumentsFromFilesResolver(mockImportService);
  }

  @Test
  public void testImportDocumentsSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(buildInput("readme.md", "# Hello"));

    ImportResult serviceResult = new ImportResult();
    serviceResult.recordSuccess("urn:li:document:readme", false);
    when(mockImportService.importDocuments(
            any(OperationContext.class),
            anyList(),
            eq(ImportUseCase.CONTEXT_DOCUMENT),
            eq(true),
            isNull(),
            eq(TEST_USER_URN)))
        .thenReturn(serviceResult);

    ImportDocumentsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCreatedCount(), Integer.valueOf(1));
    assertEquals(result.getDocumentUrns(), List.of("urn:li:document:readme"));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<DocumentCandidate>> candidatesCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockImportService)
        .importDocuments(
            any(OperationContext.class),
            candidatesCaptor.capture(),
            eq(ImportUseCase.CONTEXT_DOCUMENT),
            eq(true),
            isNull(),
            eq(TEST_USER_URN));

    DocumentCandidate candidate = candidatesCaptor.getValue().get(0);
    assertEquals(candidate.getTitle(), "Readme");
    assertEquals(candidate.getText(), "# Hello");
    assertEquals(candidate.getSourceId(), "upload.readme");
    assertEquals(candidate.getCustomProperties().get("import_source"), "file_upload");
    assertEquals(candidate.getCustomProperties().get("original_filename"), "readme.md");
    assertEquals(candidate.getCustomProperties().get("file_extension"), ".md");
  }

  @Test
  public void testImportDocumentsWithParentAndSkillUseCase() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    when(mockEnv.getContext()).thenReturn(mockContext);

    ImportDocumentsFromFilesInput input = buildInput("skill.md", "Skill body");
    input.setParentDocumentUrn(PARENT_DOCUMENT_URN);
    input.setUseCase(DocumentImportUseCase.SKILL);
    input.setShowInGlobalContext(false);
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    when(mockImportService.importDocuments(
            any(OperationContext.class),
            anyList(),
            eq(ImportUseCase.SKILL),
            eq(false),
            eq(UrnUtils.getUrn(PARENT_DOCUMENT_URN)),
            eq(TEST_USER_URN)))
        .thenReturn(new ImportResult());

    resolver.get(mockEnv).get();

    verify(mockImportService)
        .importDocuments(
            any(OperationContext.class),
            anyList(),
            eq(ImportUseCase.SKILL),
            eq(false),
            eq(UrnUtils.getUrn(PARENT_DOCUMENT_URN)),
            eq(TEST_USER_URN));
  }

  @Test
  public void testImportDocumentsSkipsBlankContent() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    when(mockEnv.getContext()).thenReturn(mockContext);

    ImportDocumentsFromFilesInput input = new ImportDocumentsFromFilesInput();
    DocumentFileInput blank = new DocumentFileInput();
    blank.setFileName("empty.txt");
    blank.setContent("   ");
    DocumentFileInput valid = new DocumentFileInput();
    valid.setFileName("valid.txt");
    valid.setContent("Body");
    input.setDocuments(List.of(blank, valid));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    when(mockImportService.importDocuments(
            any(OperationContext.class), anyList(), any(), anyBoolean(), isNull(), any(Urn.class)))
        .thenReturn(new ImportResult());

    resolver.get(mockEnv).get();

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<DocumentCandidate>> candidatesCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockImportService)
        .importDocuments(
            any(OperationContext.class),
            candidatesCaptor.capture(),
            any(),
            anyBoolean(),
            isNull(),
            any(Urn.class));

    assertEquals(candidatesCaptor.getValue().size(), 1);
    assertEquals(candidatesCaptor.getValue().get(0).getSourceId(), "upload.valid");
  }

  @Test
  public void testImportDocumentsUnauthorized() {
    QueryContext mockContext = getMockDenyContext(TEST_USER_URN.toString());
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(buildInput("readme.md", "Body"));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    verifyNoInteractions(mockImportService);
  }

  @Test
  public void testImportDocumentsServiceFailure() throws Exception {
    QueryContext mockContext = getMockAllowContext(TEST_USER_URN.toString());
    when(mockEnv.getContext()).thenReturn(mockContext);
    when(mockEnv.getArgument(eq("input"))).thenReturn(buildInput("readme.md", "Body"));

    when(mockImportService.importDocuments(
            any(OperationContext.class), anyList(), any(), anyBoolean(), isNull(), any(Urn.class)))
        .thenThrow(new RuntimeException("ingest failed"));

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testConstructorRejectsNullService() {
    assertThrows(NullPointerException.class, () -> new ImportDocumentsFromFilesResolver(null));
  }

  private static ImportDocumentsFromFilesInput buildInput(String fileName, String content) {
    ImportDocumentsFromFilesInput input = new ImportDocumentsFromFilesInput();
    DocumentFileInput document = new DocumentFileInput();
    document.setFileName(fileName);
    document.setContent(content);
    input.setDocuments(List.of(document));
    return input;
  }
}
