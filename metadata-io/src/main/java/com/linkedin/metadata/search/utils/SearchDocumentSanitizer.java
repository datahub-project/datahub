package com.linkedin.metadata.search.utils;

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
