package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.ImportDocumentsFromGitHubInput;
import com.linkedin.datahub.graphql.generated.ImportDocumentsResult;
import com.linkedin.metadata.service.docimport.DocumentImportService;
import com.linkedin.metadata.service.docimport.ImportResult;
import com.linkedin.metadata.service.docimport.ImportUseCase;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ImportDocumentsFromGitHubResolverTest {

  private DocumentImportService mockImportService;
  private ImportDocumentsFromGitHubResolver resolver;
  private DataFetchingEnvironment mockEnv;

  @BeforeMethod
  public void setup() {
    mockImportService = mock(DocumentImportService.class);
    resolver = new ImportDocumentsFromGitHubResolver(mockImportService);
    mockEnv = mock(DataFetchingEnvironment.class);
  }

  @Test
  public void testImportSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    ImportDocumentsFromGitHubInput input = new ImportDocumentsFromGitHubInput();
    input.setRepoUrl("https://github.com/acme/docs");
    input.setBranch("main");
    input.setPath("docs");
    input.setGithubToken("ghp_test123");
    input.setFileExtensions(List.of(".md"));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    ImportResult serviceResult = new ImportResult();
    serviceResult.recordSuccess("urn:li:document:doc1", false);
    serviceResult.recordSuccess("urn:li:document:doc2", false);
    serviceResult.recordSuccess("urn:li:document:doc3", true);

    when(mockImportService.importFromGitHub(
            any(OperationContext.class),
            anyString(),
            anyString(),
            anyString(),
            anyList(),
            anyString(),
            any(ImportUseCase.class),
            anyBoolean(),
            any(),
            any(Urn.class)))
        .thenReturn(serviceResult);

    ImportDocumentsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getCreatedCount(), 2);
    assertEquals(result.getUpdatedCount(), 1);
    assertEquals(result.getFailedCount(), 0);
    assertEquals(result.getDocumentUrns().size(), 3);
  }

  @Test
  public void testImportUnauthorized() {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    ImportDocumentsFromGitHubInput input = new ImportDocumentsFromGitHubInput();
    input.setRepoUrl("https://github.com/acme/docs");
    input.setGithubToken("ghp_test");
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testDefaultBranchAndPath() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    ImportDocumentsFromGitHubInput input = new ImportDocumentsFromGitHubInput();
    input.setRepoUrl("https://github.com/acme/docs");
    input.setGithubToken("ghp_test");
    // branch and path left null — should default to "main" and ""
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    ImportResult serviceResult = new ImportResult();
    when(mockImportService.importFromGitHub(
            any(OperationContext.class),
            anyString(),
            eq("main"),
            eq(""),
            anyList(),
            anyString(),
            any(ImportUseCase.class),
            anyBoolean(),
            any(),
            any(Urn.class)))
        .thenReturn(serviceResult);

    resolver.get(mockEnv).get();

    verify(mockImportService)
        .importFromGitHub(
            any(),
            anyString(),
            eq("main"),
            eq(""),
            anyList(),
            anyString(),
            any(),
            eq(true),
            any(),
            any());
  }

  @Test
  public void testToGraphQL() {
    ImportResult result = new ImportResult();
    result.recordSuccess("urn:li:document:a", false);
    result.recordSuccess("urn:li:document:b", true);
    result.recordFailure("source.c", "Network error");

    ImportDocumentsResult graphql = ImportDocumentsFromGitHubResolver.toGraphQL(result);

    assertEquals(graphql.getCreatedCount(), 1);
    assertEquals(graphql.getUpdatedCount(), 1);
    assertEquals(graphql.getFailedCount(), 1);
    assertEquals(graphql.getErrors().size(), 1);
    assertTrue(graphql.getErrors().get(0).contains("Network error"));
    assertEquals(graphql.getDocumentUrns().size(), 2);
  }
}
