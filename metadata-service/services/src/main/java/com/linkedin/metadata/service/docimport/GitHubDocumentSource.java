package com.linkedin.metadata.service.docimport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Fetches files from a GitHub repository using the GitHub REST API. Stateless — no git clone, no
 * disk I/O. Uses Java 11+ HttpClient (zero extra dependencies).
 *
 * <p>Produces {@link DocumentCandidate}s with parent-child relationships that mirror the GitHub
 * folder structure. Folder documents are generated for intermediate directories, and each candidate
 * carries a {@code parentSourceId} so the import service can wire up the hierarchy.
 */
@Slf4j
public class GitHubDocumentSource {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String GITHUB_API_BASE = "https://api.github.com";
  private static final int MAX_FILE_SIZE_BYTES = 1_000_000; // 1 MB
  private static final Pattern REPO_URL_PATTERN =
      Pattern.compile("https?://github\\.com/([^/]+/[^/]+?)(?:\\.git)?$");

  private final HttpClient httpClient;

  public GitHubDocumentSource() {
    this.httpClient =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();
  }

  @Nonnull
  public static String parseRepoIdentifier(@Nonnull String raw) {
    raw = raw.trim().replaceAll("/+$", "");
    if (raw.startsWith("http://") || raw.startsWith("https://")) {
      Matcher m = REPO_URL_PATTERN.matcher(raw);
      if (!m.matches()) {
        throw new IllegalArgumentException(
            "Could not parse GitHub URL: "
                + raw
                + ". Expected format: https://github.com/owner/repo");
      }
      return m.group(1);
    }
    if (raw.contains("/") && !raw.startsWith("/")) {
      String[] parts = raw.split("/");
      if (parts.length == 2 && !parts[0].isEmpty() && !parts[1].isEmpty()) {
        return raw;
      }
    }
    throw new IllegalArgumentException(
        "Invalid repository identifier: "
            + raw
            + ". Use 'owner/repo' or 'https://github.com/owner/repo'.");
  }

  @Nonnull
  public List<GitHubFileInfo> listFiles(
      @Nonnull String ownerRepo,
      @Nonnull String branch,
      @Nonnull String pathPrefix,
      @Nonnull List<String> extensions,
      @Nonnull String githubToken) {

    String treeUrl =
        String.format(
            "%s/repos/%s/git/trees/%s?recursive=true", GITHUB_API_BASE, ownerRepo, branch);

    JsonNode tree = executeGitHubGet(treeUrl, githubToken);
    if (tree == null || !tree.has("tree")) {
      throw new RuntimeException("Could not access branch '" + branch + "' in " + ownerRepo);
    }

    Set<String> extSet = extensions.stream().map(String::toLowerCase).collect(Collectors.toSet());
    String normalizedPrefix = pathPrefix.isEmpty() ? "" : pathPrefix.replaceAll("^/+|/+$", "");

    List<GitHubFileInfo> matching = new ArrayList<>();
    for (JsonNode item : tree.get("tree")) {
      if (!"blob".equals(item.path("type").asText())) {
        continue;
      }
      String path = item.path("path").asText();
      if (!matchesFilters(path, normalizedPrefix, extSet)) {
        continue;
      }
      Integer size = item.has("size") ? item.get("size").asInt() : null;
      matching.add(new GitHubFileInfo(path, size));
    }
    return matching;
  }

  /**
   * Fetch matching files from GitHub and produce DocumentCandidates that mirror the folder
   * hierarchy. Returns candidates in creation order: folder documents first (shallowest first),
   * then file documents. Each candidate has a {@code parentSourceId} linking it to its parent
   * folder (or null for top-level items relative to the path prefix).
   */
  @Nonnull
  public List<DocumentCandidate> fetchDocuments(
      @Nonnull String ownerRepo,
      @Nonnull String branch,
      @Nonnull String pathPrefix,
      @Nonnull List<String> extensions,
      @Nonnull String githubToken) {

    List<GitHubFileInfo> files = listFiles(ownerRepo, branch, pathPrefix, extensions, githubToken);
    if (files.isEmpty()) {
      return List.of();
    }

    String commitSha = getLatestCommitSha(ownerRepo, branch, githubToken);

    // Compute relative paths (relative to the pathPrefix)
    String normalizedPrefix = pathPrefix.isEmpty() ? "" : pathPrefix.replaceAll("^/+|/+$", "");

    // Collect all unique intermediate directories that need folder documents
    Set<String> dirPaths = collectIntermediateDirectories(files, normalizedPrefix);

    // Build folder candidates (sorted by depth — shallowest first)
    List<DocumentCandidate> candidates = new ArrayList<>();
    List<String> sortedDirs =
        dirPaths.stream()
            .sorted(Comparator.comparingInt(d -> d.split("/").length))
            .collect(Collectors.toList());

    for (String dirPath : sortedDirs) {
      Map<String, String> props = new HashMap<>();
      props.put("import_source", "github");
      props.put("github_repo", ownerRepo);
      props.put("github_branch", branch);
      props.put("github_directory_path", dirPath);
      props.put("is_folder_document", "true");

      candidates.add(
          DocumentCandidate.builder()
              .title(TextExtractors.titleFromFilename(dirPath))
              .text("")
              .sourceId(makeDirSourceId(ownerRepo, dirPath))
              .parentSourceId(resolveParentDirSourceId(ownerRepo, dirPath, normalizedPrefix))
              .customProperties(props)
              .build());
    }

    // Build file candidates
    for (GitHubFileInfo file : files) {
      String content = fetchFileContent(ownerRepo, file.getPath(), branch, githubToken);
      if (content == null) {
        continue;
      }

      Map<String, String> props = new HashMap<>();
      props.put("import_source", "github");
      props.put("github_repo", ownerRepo);
      props.put("github_branch", branch);
      props.put("github_file_path", file.getPath());
      props.put("github_commit_sha", commitSha);

      candidates.add(
          DocumentCandidate.builder()
              .title(TextExtractors.titleFromFilename(file.getPath()))
              .text(content)
              .sourceId(makeFileSourceId(ownerRepo, file.getPath()))
              .parentSourceId(resolveParentDirSourceId(ownerRepo, file.getPath(), normalizedPrefix))
              .customProperties(props)
              .build());
    }

    log.info(
        "Fetched {} documents ({} folders, {} files) from {} (branch: {}, path: {})",
        candidates.size(),
        sortedDirs.size(),
        files.size(),
        ownerRepo,
        branch,
        normalizedPrefix.isEmpty() ? "/" : normalizedPrefix);
    return candidates;
  }

  // -- Source ID generation --

  /** Source ID for a file — uses path without extension. */
  @Nonnull
  static String makeFileSourceId(@Nonnull String ownerRepo, @Nonnull String filePath) {
    String repoPart = ownerRepo.replace("/", ".");
    String pathNoExt = filePath;
    int dot = pathNoExt.lastIndexOf('.');
    if (dot > 0) {
      pathNoExt = pathNoExt.substring(0, dot);
    }
    String pathPart = pathNoExt.replace("/", ".");
    return "github." + repoPart + "." + pathPart;
  }

  /** Source ID for a directory — uses full path with _dir suffix to avoid collisions with files. */
  @Nonnull
  static String makeDirSourceId(@Nonnull String ownerRepo, @Nonnull String dirPath) {
    String repoPart = ownerRepo.replace("/", ".");
    String pathPart = dirPath.replace("/", ".");
    return "github." + repoPart + "." + pathPart + "._dir";
  }

  /**
   * Resolve the parent directory's source ID for a given path. Returns null for top-level items
   * (relative to the path prefix).
   */
  @Nullable
  private static String resolveParentDirSourceId(
      String ownerRepo, String fullPath, String pathPrefix) {
    String relativePath = relativize(fullPath, pathPrefix);
    String parentDir = parentDirectory(relativePath);
    if (parentDir == null) {
      return null;
    }
    return makeDirSourceId(ownerRepo, resolvePath(pathPrefix, parentDir));
  }

  // -- Path helpers --

  /**
   * Collect all intermediate directory paths that need folder documents. For files like
   * "docs/guides/setup.md" with prefix "docs", the relative path is "guides/setup.md" and we need
   * folder "docs/guides".
   */
  private static Set<String> collectIntermediateDirectories(
      List<GitHubFileInfo> files, String pathPrefix) {
    Set<String> dirs = new LinkedHashSet<>();
    for (GitHubFileInfo file : files) {
      String relativePath = relativize(file.getPath(), pathPrefix);
      // Walk up the path, collecting each directory level
      int lastSlash = relativePath.lastIndexOf('/');
      while (lastSlash > 0) {
        String relativeDir = relativePath.substring(0, lastSlash);
        String fullDir = resolvePath(pathPrefix, relativeDir);
        dirs.add(fullDir);
        lastSlash = relativeDir.lastIndexOf('/');
      }
    }
    return dirs;
  }

  /** Strip the prefix from a full path to get the relative path. */
  private static String relativize(String fullPath, String prefix) {
    if (prefix.isEmpty()) {
      return fullPath;
    }
    if (fullPath.startsWith(prefix + "/")) {
      return fullPath.substring(prefix.length() + 1);
    }
    if (fullPath.startsWith(prefix)) {
      return fullPath.substring(prefix.length());
    }
    return fullPath;
  }

  /** Get the parent directory of a relative path, or null if at root. */
  @Nullable
  private static String parentDirectory(String relativePath) {
    int lastSlash = relativePath.lastIndexOf('/');
    if (lastSlash <= 0) {
      return null;
    }
    return relativePath.substring(0, lastSlash);
  }

  /** Resolve a relative path against a prefix. */
  private static String resolvePath(String prefix, String relative) {
    if (prefix.isEmpty()) {
      return relative;
    }
    return prefix + "/" + relative;
  }

  // -- HTTP helpers --

  private String fetchFileContent(
      String ownerRepo, String filePath, String branch, String githubToken) {
    String url =
        String.format(
            "%s/repos/%s/contents/%s?ref=%s", GITHUB_API_BASE, ownerRepo, filePath, branch);

    JsonNode node = executeGitHubGet(url, githubToken);
    if (node == null) {
      log.warn("Failed to fetch file content for {}", filePath);
      return null;
    }

    if (node.has("size") && node.get("size").asInt() > MAX_FILE_SIZE_BYTES) {
      log.warn(
          "File {} is {} bytes (max {}), skipping",
          filePath,
          node.get("size").asInt(),
          MAX_FILE_SIZE_BYTES);
      return null;
    }

    if (node.has("content") && "base64".equals(node.path("encoding").asText())) {
      String encoded = node.get("content").asText().replaceAll("\\s", "");
      return new String(
          Base64.getDecoder().decode(encoded), java.nio.charset.StandardCharsets.UTF_8);
    }

    if (node.has("download_url") && !node.get("download_url").isNull()) {
      return fetchRawContent(node.get("download_url").asText(), githubToken);
    }

    return null;
  }

  private String fetchRawContent(String downloadUrl, String githubToken) {
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(downloadUrl))
              .header("Authorization", "Bearer " + githubToken)
              .timeout(Duration.ofSeconds(30))
              .GET()
              .build();
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 200) {
        return response.body();
      }
      log.warn(
          "Failed to download raw content from {}: HTTP {}", downloadUrl, response.statusCode());
      return null;
    } catch (Exception e) {
      log.warn("Failed to download raw content from {}: {}", downloadUrl, e.getMessage());
      return null;
    }
  }

  private String getLatestCommitSha(String ownerRepo, String branch, String githubToken) {
    String url = String.format("%s/repos/%s/branches/%s", GITHUB_API_BASE, ownerRepo, branch);
    JsonNode node = executeGitHubGet(url, githubToken);
    if (node != null && node.has("commit")) {
      return node.path("commit").path("sha").asText("unknown");
    }
    return "unknown";
  }

  private JsonNode executeGitHubGet(String url, String githubToken) {
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .header("Authorization", "Bearer " + githubToken)
              .header("Accept", "application/vnd.github+json")
              .header("X-GitHub-Api-Version", "2022-11-28")
              .timeout(Duration.ofSeconds(30))
              .GET()
              .build();
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        log.warn("GitHub API returned HTTP {} for {}", response.statusCode(), url);
        return null;
      }
      return MAPPER.readTree(response.body());
    } catch (IOException | InterruptedException e) {
      log.warn("GitHub API request failed for {}: {}", url, e.getMessage());
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      return null;
    }
  }

  private static boolean matchesFilters(
      String filePath, String pathPrefix, Set<String> extensions) {
    if (!pathPrefix.isEmpty() && !filePath.startsWith(pathPrefix)) {
      return false;
    }
    int dot = filePath.lastIndexOf('.');
    if (dot < 0) {
      return false;
    }
    String ext = filePath.substring(dot).toLowerCase();
    return extensions.contains(ext);
  }
}
