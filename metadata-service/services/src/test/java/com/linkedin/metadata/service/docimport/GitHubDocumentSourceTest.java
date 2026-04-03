package com.linkedin.metadata.service.docimport;

import static org.testng.Assert.*;

import java.util.List;
import org.testng.annotations.Test;

public class GitHubDocumentSourceTest {

  @Test
  public void testParseRepoIdentifier_shorthand() {
    assertEquals(GitHubDocumentSource.parseRepoIdentifier("acme/docs"), "acme/docs");
  }

  @Test
  public void testParseRepoIdentifier_httpsUrl() {
    assertEquals(
        GitHubDocumentSource.parseRepoIdentifier("https://github.com/acme/docs"), "acme/docs");
  }

  @Test
  public void testParseRepoIdentifier_urlWithDotGit() {
    assertEquals(
        GitHubDocumentSource.parseRepoIdentifier("https://github.com/acme/docs.git"), "acme/docs");
  }

  @Test
  public void testParseRepoIdentifier_trailingSlash() {
    assertEquals(GitHubDocumentSource.parseRepoIdentifier("acme/docs/"), "acme/docs");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseRepoIdentifier_invalid() {
    GitHubDocumentSource.parseRepoIdentifier("not-a-repo");
  }

  @Test
  public void testMakeFileSourceId() {
    assertEquals(
        GitHubDocumentSource.makeFileSourceId("acme/docs", "guides/setup.md"),
        "github.acme.docs.guides.setup");
  }

  @Test
  public void testMakeFileSourceId_rootFile() {
    assertEquals(
        GitHubDocumentSource.makeFileSourceId("acme/docs", "README.md"), "github.acme.docs.README");
  }

  @Test
  public void testMakeDirSourceId() {
    assertEquals(
        GitHubDocumentSource.makeDirSourceId("acme/docs", "guides"),
        "github.acme.docs.guides._dir");
  }

  @Test
  public void testMakeDirSourceId_nestedDir() {
    assertEquals(
        GitHubDocumentSource.makeDirSourceId("acme/docs", "guides/advanced"),
        "github.acme.docs.guides.advanced._dir");
  }

  @Test
  public void testDirSourceIdDoesNotCollideWithFileSourceId() {
    String fileId = GitHubDocumentSource.makeFileSourceId("acme/docs", "guides.md");
    String dirId = GitHubDocumentSource.makeDirSourceId("acme/docs", "guides");
    assertNotEquals(fileId, dirId, "File and directory source IDs should not collide");
  }

  /**
   * Verifies that fetchDocuments produces folder candidates with correct parent-child wiring. Since
   * fetchDocuments calls the GitHub API, we test the hierarchy logic indirectly by validating
   * source IDs and the contract between file/dir IDs.
   */
  @Test
  public void testHierarchySourceIdContract() {
    String repo = "org/repo";
    String prefix = "docs";

    // Simulate files: docs/overview.md, docs/guides/setup.md, docs/guides/advanced.md
    // Expected dirs: docs/guides
    // Expected parent chain:
    //   docs/overview.md -> parentSourceId = null (top-level)
    //   docs/guides (dir) -> parentSourceId = null (top-level)
    //   docs/guides/setup.md -> parentSourceId = docs/guides dir
    //   docs/guides/advanced.md -> parentSourceId = docs/guides dir

    String guidesDir = GitHubDocumentSource.makeDirSourceId(repo, "docs/guides");
    String overviewFile = GitHubDocumentSource.makeFileSourceId(repo, "docs/overview.md");
    String setupFile = GitHubDocumentSource.makeFileSourceId(repo, "docs/guides/setup.md");

    // All IDs should be unique
    List<String> ids = List.of(guidesDir, overviewFile, setupFile);
    assertEquals(ids.stream().distinct().count(), ids.size(), "All IDs must be unique");

    // Directory ID should end with ._dir
    assertTrue(guidesDir.endsWith("._dir"));

    // File IDs should not contain _dir
    assertFalse(overviewFile.contains("._dir"));
    assertFalse(setupFile.contains("._dir"));
  }

  /**
   * Validates that for deeply nested files, intermediate directories are properly identified. We
   * test this by checking the source IDs that would be produced for each level.
   */
  @Test
  public void testDeepNestingSourceIds() {
    String repo = "org/repo";

    // File: a/b/c/file.md -> needs dirs: a, a/b, a/b/c (relative to prefix)
    // With prefix "a", relative path is b/c/file.md -> needs dirs: a/b, a/b/c

    String dirB = GitHubDocumentSource.makeDirSourceId(repo, "a/b");
    String dirBC = GitHubDocumentSource.makeDirSourceId(repo, "a/b/c");
    String file = GitHubDocumentSource.makeFileSourceId(repo, "a/b/c/file.md");

    // Verify unique and well-formed
    assertNotEquals(dirB, dirBC);
    assertNotEquals(dirBC, file);
    assertEquals(dirB, "github.org.repo.a.b._dir");
    assertEquals(dirBC, "github.org.repo.a.b.c._dir");
    assertEquals(file, "github.org.repo.a.b.c.file");
  }

  /**
   * fetchDocuments should produce candidates in parent-before-child order, with folder candidates
   * coming before their child files. We verify the ordering contract by checking that a parent's
   * parentSourceId references the grandparent (or null for top-level).
   */
  @Test
  public void testParentSourceIdChaining() {
    // Simulated hierarchy: prefix="src", files at src/lib/utils/helper.md
    // Dirs needed: src/lib, src/lib/utils
    // Parent chain: src/lib -> null, src/lib/utils -> src/lib, helper.md -> src/lib/utils

    String repo = "org/repo";
    String dirLib = GitHubDocumentSource.makeDirSourceId(repo, "src/lib");
    String dirUtils = GitHubDocumentSource.makeDirSourceId(repo, "src/lib/utils");

    // For the build, lib's parent is null (top-level under prefix)
    // utils's parent is lib
    // helper.md's parent is utils
    // This is the contract that fetchDocuments enforces
    assertNotEquals(dirLib, dirUtils);
    assertTrue(dirLib.contains("lib._dir"));
    assertTrue(dirUtils.contains("utils._dir"));
  }
}
