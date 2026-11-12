package com.linkedin.metadata.config.usage.cigate;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

public final class GitChangeDetector {

  static final String BASE_SHA_ENV = "GITHUB_BASE_SHA";
  static final String CI_ENV = "CI";

  private static final long GIT_TIMEOUT_SECONDS = 60;

  private GitChangeDetector() {}

  public static boolean hasChanges(@Nonnull Path repoRoot, @Nonnull Predicate<String> pathMatcher) {
    return hasChanges(repoRoot, pathMatcher, System.getenv());
  }

  static boolean hasChanges(
      @Nonnull Path repoRoot,
      @Nonnull Predicate<String> pathMatcher,
      @Nonnull Map<String, String> env) {
    String baseSha = env.get(BASE_SHA_ENV);
    List<List<String>> commands = new ArrayList<>();
    if (baseSha != null && !baseSha.isBlank()) {
      commands.add(List.of("git", "diff", "--name-only", baseSha, "HEAD"));
    } else if ("true".equalsIgnoreCase(env.get(CI_ENV))) {
      // A CI checkout has no uncommitted changes, so a working-tree diff would always be empty and
      // the gate would only run when git happened to fail. Without a base to diff against, scan
      // everything so the result does not depend on git timing.
      System.err.println("Usage registry gate: CI without " + BASE_SHA_ENV + ", scanning all");
      return true;
    } else {
      commands.add(List.of("git", "diff", "--name-only", "HEAD"));
      commands.add(List.of("git", "diff", "--name-only", "--cached", "HEAD"));
    }

    for (List<String> command : commands) {
      try {
        for (String path : runGit(repoRoot, command)) {
          if (pathMatcher.test(path)) {
            return true;
          }
        }
      } catch (IOException | InterruptedException e) {
        // Fail open: run the gate when git is unavailable (missing .git, bad base SHA, etc.)
        // so validation is never silently skipped. Use -P*UsageCheckForce=true to force locally.
        System.err.println("Usage registry gate: " + e.getMessage() + ", scanning all");
        return true;
      }
    }
    return false;
  }

  @Nonnull
  private static List<String> runGit(@Nonnull Path repoRoot, @Nonnull List<String> command)
      throws IOException, InterruptedException {
    // Output goes to a file rather than a pipe: reading a pipe only after waitFor() blocks git
    // once its output fills the pipe buffer, which then surfaces as a timeout.
    File output = File.createTempFile("usage-gate-git", ".out");
    File errors = File.createTempFile("usage-gate-git", ".err");
    try {
      Process process =
          new ProcessBuilder(command)
              .directory(repoRoot.toFile())
              .redirectOutput(output)
              .redirectError(errors)
              .start();
      if (!process.waitFor(GIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        throw new IOException("git command timed out: " + String.join(" ", command));
      }
      if (process.exitValue() != 0) {
        throw new IOException(
            "git command failed: "
                + String.join(" ", command)
                + ": "
                + Files.readString(errors.toPath(), StandardCharsets.UTF_8).trim());
      }
      List<String> lines = new ArrayList<>();
      for (String line : Files.readAllLines(output.toPath(), StandardCharsets.UTF_8)) {
        if (!line.isBlank()) {
          lines.add(line.trim());
        }
      }
      return lines;
    } finally {
      Files.deleteIfExists(output.toPath());
      Files.deleteIfExists(errors.toPath());
    }
  }
}
