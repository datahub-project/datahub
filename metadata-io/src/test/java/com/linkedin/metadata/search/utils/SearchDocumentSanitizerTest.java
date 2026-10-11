package com.linkedin.metadata.search.utils;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class SearchDocumentSanitizerTest {

  @Test
  public void testPreserveExternalImageLinks() {
    String input = "![diagram](https://example.com/image.png) is good";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    // External URLs should be preserved
    assertEquals(result, input);
  }

  @Test
  public void testPreservePlainText() {
    String input = "This is a plain description without images";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    assertEquals(result, input);
  }

  @Test
  public void testNullAndEmptyStrings() {
    assertNull(SearchDocumentSanitizer.sanitizeForIndexing(null));
    assertEquals(SearchDocumentSanitizer.sanitizeForIndexing(""), "");
    assertEquals(SearchDocumentSanitizer.sanitizeForIndexing("   "), "   ");
  }

  @Test
  public void testLargeBase64Image() {
    // Simulate 1.4MB base64 string (actual issue from logs)
    StringBuilder largeBase64 = new StringBuilder("![large](data:image/png;base64,");
    for (int i = 0; i < 1_400_000; i++) {
      largeBase64.append((char) ('A' + (i % 26)));
    }
    largeBase64.append(")");

    String input = "Description " + largeBase64 + " more text";

    long startTime = System.nanoTime();
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);
    long duration = System.nanoTime() - startTime;

    // Should remove the large image
    assertFalse(result.contains("data:image"));
    assertTrue(result.contains("[Image: large]"));
    assertTrue(result.length() < 100, "Result length: " + result.length());
  }

  @Test
  public void testContainsBase64Images() {
    assertTrue(SearchDocumentSanitizer.containsBase64Images("![](data:image/png;base64,ABC)"));
    assertTrue(
        SearchDocumentSanitizer.containsBase64Images("<img src=\"data:image/jpeg;base64,XYZ\">"));
    assertFalse(SearchDocumentSanitizer.containsBase64Images("Plain text"));
    assertFalse(SearchDocumentSanitizer.containsBase64Images("![](https://example.com/img.png)"));
    assertFalse(SearchDocumentSanitizer.containsBase64Images(null));
  }

  @Test
  public void testIdempotency() {
    String input = "![img](data:image/png;base64,ABC) text";

    String result1 = SearchDocumentSanitizer.sanitizeForIndexing(input);
    String result2 = SearchDocumentSanitizer.sanitizeForIndexing(result1);

    // Sanitizing twice should produce same result
    assertEquals(result1, result2);
  }

  @Test
  public void testMultipleLargeImages() {
    // Create multiple large images (simulating real-world scenario)
    StringBuilder input = new StringBuilder("Start ");

    for (int i = 0; i < 3; i++) {
      input.append("![img").append(i).append("](data:image/png;base64,");
      // Add 100KB base64 string per image
      for (int j = 0; j < 100_000; j++) {
        input.append((char) ('A' + (j % 26)));
      }
      input.append(") ");
    }
    input.append("End");

    long startTime = System.nanoTime();
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input.toString());
    long duration = System.nanoTime() - startTime;

    // Should remove all images
    assertFalse(result.contains("base64"));
    assertTrue(result.contains("[Image: img0]"));
    assertTrue(result.contains("[Image: img1]"));
    assertTrue(result.contains("[Image: img2]"));
    assertTrue(result.contains("Start"));
    assertTrue(result.contains("End"));
  }

  @Test
  public void testContainsBase64ImagesWithAngleBrackets() {
    assertTrue(
        SearchDocumentSanitizer.containsBase64Images("![](<data:image/png;base64,iVBORw0KGgo>)"));
    assertTrue(
        SearchDocumentSanitizer.containsBase64Images("![alt](<data:image/jpeg;base64,/9j/4AAQ>)"));
  }
}
