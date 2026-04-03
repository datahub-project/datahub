package com.linkedin.metadata.service.docimport;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Server-side text extraction for plain-text and HTML file formats. DOCX and PDF parsing is handled
 * client-side (browser) using mammoth.js / equivalent libraries, so only text-based formats are
 * supported here.
 */
@Slf4j
public final class TextExtractors {

  private TextExtractors() {}

  @FunctionalInterface
  interface Extractor {
    String extract(byte[] content, String filename) throws Exception;
  }

  private static final Map<String, Extractor> EXTRACTORS;

  static {
    Map<String, Extractor> m = new LinkedHashMap<>();
    m.put(".txt", TextExtractors::extractPlainText);
    m.put(".md", TextExtractors::extractPlainText);
    m.put(".markdown", TextExtractors::extractPlainText);
    m.put(".rst", TextExtractors::extractPlainText);
    m.put(".csv", TextExtractors::extractPlainText);
    m.put(".json", TextExtractors::extractPlainText);
    m.put(".yaml", TextExtractors::extractPlainText);
    m.put(".yml", TextExtractors::extractPlainText);
    m.put(".html", TextExtractors::extractHtml);
    m.put(".htm", TextExtractors::extractHtml);
    EXTRACTORS = Collections.unmodifiableMap(m);
  }

  private static final Pattern HTML_TAGS = Pattern.compile("<[^>]+>");
  private static final Pattern HTML_ENTITIES =
      Pattern.compile("&(amp|lt|gt|quot|apos|nbsp|#\\d+|#x[\\da-fA-F]+);");
  private static final Pattern STRIP_BLOCKS =
      Pattern.compile(
          "<(script|style|nav|footer|header)[^>]*>.*?</\\1>",
          Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
  private static final Pattern WHITESPACE_RUNS = Pattern.compile("\\n{3,}");

  /** Returns the list of supported file extensions (lowercase, with dot). */
  @Nonnull
  public static List<String> getSupportedExtensions() {
    return EXTRACTORS.keySet().stream().sorted().collect(Collectors.toList());
  }

  /** Returns true if the given extension is supported for text extraction. */
  public static boolean isSupported(@Nonnull String extension) {
    return EXTRACTORS.containsKey(extension.toLowerCase());
  }

  /**
   * Extract text from file content based on file extension.
   *
   * @return extracted text, or null if the format is unsupported
   */
  @Nullable
  public static String extract(@Nonnull byte[] content, @Nonnull String filename) {
    String ext = getExtension(filename);
    Extractor extractor = EXTRACTORS.get(ext);
    if (extractor == null) {
      log.warn("Unsupported file format: {}. Supported: {}", filename, getSupportedExtensions());
      return null;
    }
    try {
      String text = extractor.extract(content, filename);
      return (text != null && !text.isBlank()) ? text : null;
    } catch (Exception e) {
      log.warn("Failed to extract text from {}: {}", filename, e.getMessage());
      return null;
    }
  }

  static String extractPlainText(byte[] content, String filename) {
    return new String(content, StandardCharsets.UTF_8);
  }

  /**
   * Extracts visible text from HTML by stripping script/style/nav blocks, HTML tags, and decoding
   * entities. Uses Java stdlib only (no Jsoup dependency).
   */
  static String extractHtml(byte[] content, String filename) {
    String html = new String(content, StandardCharsets.UTF_8);
    html = STRIP_BLOCKS.matcher(html).replaceAll("");
    html = html.replaceAll("(?i)<(br|p|div|h[1-6]|li|tr)[^>]*>", "\n");
    html = HTML_TAGS.matcher(html).replaceAll("");
    html = decodeHtmlEntities(html);
    html = WHITESPACE_RUNS.matcher(html.trim()).replaceAll("\n\n");
    return html;
  }

  // -- Helpers --

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

  private static String decodeHtmlEntities(String text) {
    text = text.replace("&amp;", "&");
    text = text.replace("&lt;", "<");
    text = text.replace("&gt;", ">");
    text = text.replace("&quot;", "\"");
    text = text.replace("&apos;", "'");
    text = text.replace("&nbsp;", " ");
    return text;
  }
}
