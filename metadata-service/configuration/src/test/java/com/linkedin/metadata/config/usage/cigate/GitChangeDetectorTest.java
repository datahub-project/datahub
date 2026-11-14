package com.linkedin.metadata.config.usage.cigate;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GitChangeDetectorTest {

  private static final String WATCHED = "service/src/main/java/Controller.java";
  private static final Predicate<String> MATCHER =
      path -> path.startsWith("service/src/main/java/");

  private Path repo;

  @BeforeMethod
  public void setUp() throws Exception {
    repo = Files.createTempDirectory("git-change-detector");
    git("init", "-q");
    git("config", "user.email", "test@example.com");
    git("config", "user.name", "test");
    git("config", "commit.gpgsign", "false");
    write(WATCHED, "class Controller {}");
    write("other/gradle.lockfile", "a=1");
    git("add", ".");
    git("commit", "-q", "-m", "base");
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws IOException {
    try (var paths = Files.walk(repo)) {
      paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    }
  }

  @Test
  public void testCiWithoutBaseShaScansEverythingEvenOnCleanCheckout() {
    assertTrue(GitChangeDetector.hasChanges(repo, MATCHER, Map.of("CI", "true")));
  }

  @Test
  public void testLocalCleanCheckoutSkips() {
    assertFalse(GitChangeDetector.hasChanges(repo, MATCHER, Map.of()));
  }

  @Test
  public void testLocalUncommittedWatchedChangeIsDetected() throws Exception {
    write(WATCHED, "class Controller { void handler() {} }");
    assertTrue(GitChangeDetector.hasChanges(repo, MATCHER, Map.of()));
  }

  @Test
  public void testBaseShaOnlyCountsWatchedPaths() throws Exception {
    String base = git("rev-parse", "HEAD").trim();
    write("other/gradle.lockfile", "a=2");
    git("commit", "-q", "-am", "lockfile only");
    assertFalse(
        GitChangeDetector.hasChanges(repo, MATCHER, Map.of("CI", "true", "GITHUB_BASE_SHA", base)));

    write(WATCHED, "class Controller { void handler() {} }");
    git("commit", "-q", "-am", "controller");
    assertTrue(
        GitChangeDetector.hasChanges(repo, MATCHER, Map.of("CI", "true", "GITHUB_BASE_SHA", base)));
  }

  @Test
  public void testGitFailureFailsOpen() throws Exception {
    assertTrue(
        GitChangeDetector.hasChanges(
            repo,
            MATCHER,
            Map.of("CI", "true", "GITHUB_BASE_SHA", "0000000000000000000000000000000000000000")));
    Path notARepo = Files.createTempDirectory("not-a-git-repo");
    try {
      assertTrue(GitChangeDetector.hasChanges(notARepo, MATCHER, Map.of()));
    } finally {
      Files.deleteIfExists(notARepo);
    }
  }

  @Test
  public void testLargeDiffDoesNotStallOnOutput() throws Exception {
    // Enough changed paths to overflow a 64 KiB pipe buffer; none of them are watched.
    for (int i = 0; i < 1500; i++) {
      write("other/generated/some/deeply/nested/package/path/File" + i + ".txt", "x");
    }
    git("add", ".");
    assertFalse(GitChangeDetector.hasChanges(repo, MATCHER, Map.of()));
  }

  private void write(String relative, String content) throws IOException {
    Path file = repo.resolve(relative);
    Files.createDirectories(file.getParent());
    Files.writeString(file, content, StandardCharsets.UTF_8);
  }

  private String git(String... args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add("git");
    command.addAll(List.of(args));
    Path out = Files.createTempFile("git-test", ".out");
    try {
      Process process =
          new ProcessBuilder(command)
              .directory(repo.toFile())
              .redirectErrorStream(true)
              .redirectOutput(out.toFile())
              .start();
      if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
        throw new IllegalStateException(
            "git " + String.join(" ", args) + " failed: " + Files.readString(out));
      }
      return Files.readString(out);
    } finally {
      Files.deleteIfExists(out);
    }
  }
}
