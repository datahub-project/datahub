package com.linkedin.datahub.graphql.resolvers.knowledge;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GitHubFilePreview;
import com.linkedin.datahub.graphql.generated.ImportDocumentsFromGitHubInput;
import com.linkedin.datahub.graphql.generated.PreviewDocumentsFromGitHubResult;
import com.linkedin.metadata.service.docimport.DocumentImportService;
import com.linkedin.metadata.service.docimport.GitHubFileInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PreviewDocumentsFromGitHubResolverTest {

  private DocumentImportService mockImportService;
  private PreviewDocumentsFromGitHubResolver resolver;
  private DataFetchingEnvironment mockEnv;

  @BeforeMethod
  public void setup() {
    mockImportService = mock(DocumentImportService.class);
    resolver = new PreviewDocumentsFromGitHubResolver(mockImportService);
    mockEnv = mock(DataFetchingEnvironment.class);
  }

  @Test
  public void testPreviewSuccess() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    ImportDocumentsFromGitHubInput input = new ImportDocumentsFromGitHubInput();
    input.setRepoUrl("https://github.com/acme/docs");
    input.setBranch("main");
    input.setPath("docs");
    input.setGithubToken("ghp_test");
    input.setFileExtensions(List.of(".md"));
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    List<GitHubFileInfo> serviceResult =
        List.of(
            new GitHubFileInfo("docs/readme.md", 1024), new GitHubFileInfo("docs/guide.md", 2048));

    when(mockImportService.previewGitHubImport(
            anyString(), anyString(), anyString(), anyList(), anyString()))
        .thenReturn(serviceResult);

    PreviewDocumentsFromGitHubResult result = resolver.get(mockEnv).get();

    assertEquals(result.getTotalCount(), 2);
    assertEquals(result.getFiles().size(), 2);
    assertEquals(result.getFiles().get(0).getPath(), "docs/readme.md");
    assertEquals(result.getFiles().get(0).getSize(), Integer.valueOf(1024));
  }

  @Test
  public void testPreviewUnauthorized() {
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    ImportDocumentsFromGitHubInput input = new ImportDocumentsFromGitHubInput();
    input.setRepoUrl("https://github.com/acme/docs");
    input.setGithubToken("ghp_test");
    when(mockEnv.getArgument(eq("input"))).thenReturn(input);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testToGraphQL() {
    List<GitHubFileInfo> files =
        List.of(new GitHubFileInfo("readme.md", 512), new GitHubFileInfo("guide.txt", null));

    PreviewDocumentsFromGitHubResult result = PreviewDocumentsFromGitHubResolver.toGraphQL(files);

    assertEquals(result.getTotalCount(), 2);
    List<GitHubFilePreview> previews = result.getFiles();
    assertEquals(previews.get(0).getPath(), "readme.md");
    assertEquals(previews.get(0).getSize(), Integer.valueOf(512));
    assertEquals(previews.get(1).getPath(), "guide.txt");
    assertNull(previews.get(1).getSize());
  }
}
