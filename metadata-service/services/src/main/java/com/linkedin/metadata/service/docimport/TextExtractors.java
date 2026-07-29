package com.linkedin.metadata.service.docimport;

import java.util.Arrays;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Filename helpers for the file-upload document import path. Text extraction (plain text, HTML,
 * DOCX, PDF) is performed client-side in the browser before calling {@code
 * importDocumentsFromFiles}.
 */
@Slf4j
public final class TextExtractors {

  private TextExtractors() {}

  @Nonnull
  public static String getExtension(@Nonnull String filename) {
    int dot = filename.lastIndexOf('.');
    return dot >= 0 ? filename.substring(dot).toLowerCase() : "";
  }

  /** Derive a human-readable title from a filename. */
  @Nonnull
  public static String titleFromFilename(@Nonnull String filename) {
    String base = filename;
    int slash = base.lastIndexOf('/');
    if (slash >= 0) base = base.substring(slash + 1);
    int dot = base.lastIndexOf('.');
    if (dot > 0) base = base.substring(0, dot);
    String[] words = base.replace("-", " ").replace("_", " ").trim().split("\\s+");
    return Arrays.stream(words)
        .filter(w -> !w.isEmpty())
        .map(w -> Character.toUpperCase(w.charAt(0)) + w.substring(1).toLowerCase())
        .collect(Collectors.joining(" "));
  }
}
