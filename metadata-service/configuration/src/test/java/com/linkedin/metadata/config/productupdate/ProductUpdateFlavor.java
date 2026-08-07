package com.linkedin.metadata.config.productupdate;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

/**
 * The two bundled product update ("What's New" toast) JSONs and the release history each one
 * tracks.
 *
 * <p>Core and cloud version independently, so each flavor resolves its own latest release. Both
 * release histories keep unreleased notes under a draft heading ({@code ## Next} / {@code next.md})
 * that is promoted to a versioned one at release cut, so drafting notes on master does not make a
 * flavor look out of date — only actually cutting a release does.
 */
public enum ProductUpdateFlavor {
  CORE(
      "DataHub Core",
      "metadata-service/configuration/src/main/resources/product-update.json",
      "docs/how/updating-datahub.md") {
    @Override
    @Nonnull
    Optional<ReleaseVersion> findLatestRelease(@Nonnull Path repoRoot) {
      Path releaseNotes = repoRoot.resolve(releaseSource());
      if (!Files.isRegularFile(releaseNotes)) {
        return Optional.empty();
      }
      Matcher headings = VERSIONED_HEADING.matcher(readString(releaseNotes));
      Optional<ReleaseVersion> latest = Optional.empty();
      while (headings.find()) {
        latest = higherOf(latest, ReleaseVersion.parse(headings.group(1)));
      }
      return latest;
    }
  },

  CLOUD(
      "DataHub Cloud",
      "metadata-service/configuration/src/main/resources/product-update-saas.json",
      "docs/managed-datahub/release-notes") {
    @Override
    @Nonnull
    Optional<ReleaseVersion> findLatestRelease(@Nonnull Path repoRoot) {
      Path releaseNotesDir = repoRoot.resolve(releaseSource());
      if (!Files.isDirectory(releaseNotesDir)) {
        return Optional.empty();
      }
      try (Stream<Path> notes = Files.list(releaseNotesDir)) {
        return notes
            .map(note -> VERSIONED_NOTE_FILE.matcher(note.getFileName().toString()))
            .filter(Matcher::matches)
            .map(matcher -> ReleaseVersion.parse(matcher.group(1)))
            .flatMap(Optional::stream)
            .max(Comparator.naturalOrder());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  };

  /**
   * A released section in updating-datahub.md. Supports both the current {@code ## v1.7.0} and
   * historical {@code ## 1.3.0} convention, plus optional annotations after the version.
   */
  private static final Pattern VERSIONED_HEADING =
      Pattern.compile("(?m)^##[ \\t]+(v?\\d+(?:[._]\\d+)+)(?:[ \\t]+[^\\r\\n]*)?$");

  /** A released cloud release note, e.g. "v_2_1_0.md". Skips "next.md". */
  private static final Pattern VERSIONED_NOTE_FILE = Pattern.compile("(v_[\\d_]+)\\.md");

  private final String label;
  private final String jsonPath;
  private final String releaseSource;

  ProductUpdateFlavor(
      @Nonnull String label, @Nonnull String jsonPath, @Nonnull String releaseSource) {
    this.label = label;
    this.jsonPath = jsonPath;
    this.releaseSource = releaseSource;
  }

  @Nonnull
  abstract Optional<ReleaseVersion> findLatestRelease(@Nonnull Path repoRoot);

  /** The latest released version for this flavor, failing loudly if none can be resolved. */
  @Nonnull
  public ReleaseVersion latestRelease(@Nonnull Path repoRoot) {
    return findLatestRelease(repoRoot)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Found no released "
                        + label
                        + " version in "
                        + releaseSource
                        + " under repo root "
                        + repoRoot));
  }

  @Nonnull
  public String label() {
    return label;
  }

  @Nonnull
  public String jsonPath() {
    return jsonPath;
  }

  @Nonnull
  public String releaseSource() {
    return releaseSource;
  }

  @Nonnull
  private static Optional<ReleaseVersion> higherOf(
      @Nonnull Optional<ReleaseVersion> left, @Nonnull Optional<ReleaseVersion> right) {
    if (left.isEmpty()) {
      return right;
    }
    if (right.isEmpty()) {
      return left;
    }
    return Optional.of(left.get().compareTo(right.get()) >= 0 ? left.get() : right.get());
  }

  @Nonnull
  private static String readString(@Nonnull Path path) {
    try {
      return Files.readString(path);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
