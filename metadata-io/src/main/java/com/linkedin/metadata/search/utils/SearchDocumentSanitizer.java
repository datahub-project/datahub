package com.linkedin.metadata.search.utils;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Utility class for sanitizing search document content before indexing to OpenSearch/Elasticsearch.
 *
 * <p>Primary purpose: Remove base64-encoded images from text fields to prevent indexing failures
 * due to the 32,766-byte term limit in analyzed text fields.
 *
 * <p>This class handles:
 *
 * <ul>
 *   <li>Markdown images: {@code ![alt text](data:image/png;base64,...)}
 *   <li>HTML img tags: {@code <img src="data:image/png;base64,...">}
 * </ul>
 *
 * <p>External image URLs (https://, http://) are preserved.
 */
public class SearchDocumentSanitizer {

  private static final Pattern BASE64_IMAGE_PATTERN =
      Pattern.compile(
          "!\\[([^\\]]*)\\]\\(<?data:image/[^;]+;base64,[A-Za-z0-9+/=\\s]*>?\\)",
          Pattern.DOTALL | Pattern.MULTILINE);

  private static final Pattern HTML_IMG_BASE64_PATTERN =
      Pattern.compile(
          "<img[^>]+src=[\"']data:image/[^;]+;base64,[A-Za-z0-9+/=\\s]*[\"'][^>]*>",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  private static final int MIN_LENGTH_FOR_SANITIZATION = 1000;

  private static final String BASE64_IMAGE_MARKER = "data:image";

  @Nullable
  public static String sanitizeForIndexing(@Nullable String value) {
    if (value == null || value.isEmpty()) {
      return value;
    }

    if (value.length() < MIN_LENGTH_FOR_SANITIZATION) {
      return value;
    }

    if (!value.contains(BASE64_IMAGE_MARKER)) {
      return value;
    }
    // Remove markdown base64 images, preserving alt text
    String sanitized =
        BASE64_IMAGE_PATTERN
            .matcher(value)
            .replaceAll(
                matchResult -> {
                  String altText = matchResult.group(1);
                  // If alt text exists, preserve it in readable form
                  // Otherwise, remove the image entirely
                  return altText.isEmpty() ? "" : "[Image: " + altText + "]";
                });

    // Remove HTML img tags with base64 data URLs
    sanitized = HTML_IMG_BASE64_PATTERN.matcher(sanitized).replaceAll("[Image]");

    return sanitized.trim();
  }

  /**
   * Truncates a string so that its UTF-8 encoding is at most {@code maxBytes} bytes, cutting on a
   * code-point boundary so a multi-byte character is never split.
   *
   * <p>Analyzed-text fields ({@code TEXT}, {@code TEXT_PARTIAL}, {@code WORD_GRAM}) are backed by a
   * {@code keyword} parent in the Elasticsearch mapping, which enforces Lucene's hard 32,766-byte
   * per-term limit. A value over that limit is rejected by OpenSearch and the entire document write
   * is dropped, silently removing the document from search. Truncating the indexed value keeps the
   * document searchable; the full value is unaffected in primary storage.
   */
  @Nullable
  public static String truncateForIndexing(@Nullable String value, int maxBytes) {
    if (value == null || value.isEmpty()) {
      return value;
    }
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    if (utf8.length <= maxBytes) {
      return value;
    }
    // Walk back off any UTF-8 continuation byte (10xxxxxx) so we don't slice through a character.
    int end = maxBytes;
    while (end > 0 && (utf8[end] & 0xC0) == 0x80) {
      end--;
    }
    return new String(utf8, 0, end, StandardCharsets.UTF_8);
  }

  public static boolean containsBase64Images(@Nullable String value) {
    if (value == null || value.isEmpty()) {
      return false;
    }
    if (!value.contains(BASE64_IMAGE_MARKER)) {
      return false;
    }
    return BASE64_IMAGE_PATTERN.matcher(value).find()
        || HTML_IMG_BASE64_PATTERN.matcher(value).find();
  }
}
