package com.linkedin.metadata.config.usage.cigate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nonnull;

public final class GitChangeDetector {

  private GitChangeDetector() {}

  public static boolean hasChanges(@Nonnull Path repoRoot, @Nonnull Predicate<String> pathMatcher) {
    List<String> commands = new ArrayList<>();
    String baseSha = System.getenv("GITHUB_BASE_SHA");
    if (baseSha != null && !baseSha.isBlank()) {
      commands.add("git diff --name-only " + baseSha + " HEAD");
    } else {
      commands.add("git diff --name-only HEAD");
      commands.add("git diff --name-only --cached HEAD");
    }

    for (String command : commands) {
      try {
        for (String path : runGit(repoRoot, command)) {
          if (pathMatcher.test(path)) {
            return true;
          }
        }
      } catch (IOException | InterruptedException e) {
        // Fail open: run the gate when git is unavailable (shallow clone, missing .git, etc.)
        // so CI never silently skips validation. Use -P*UsageCheckForce=true to force locally.
        return true;
      }
    }
    return false;
  }

  @Nonnull
  private static List<String> runGit(@Nonnull Path repoRoot, @Nonnull String command)
      throws IOException, InterruptedException {
    Process process =
        new ProcessBuilder("bash", "-lc", command)
            .directory(repoRoot.toFile())
            .redirectErrorStream(true)
            .start();
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
      process.destroyForcibly();
      throw new IOException("git command timed out: " + command);
    }
    List<String> lines = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.isBlank()) {
          lines.add(line.trim());
        }
      }
    }
    if (process.exitValue() != 0) {
      throw new IOException("git command failed: " + command);
    }
    return lines;
  }
}
