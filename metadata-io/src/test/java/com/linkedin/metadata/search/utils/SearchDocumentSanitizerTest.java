package com.linkedin.metadata.search.utils;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

public class SearchDocumentSanitizerTest {

  @Test
  public void testSanitizeMarkdownBase64Image() {
    String input =
        "Description with ![diagram](data:image/png;base64,iVBORw0KGgoAAAANSUhEUg) image";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    // Should preserve alt text but remove data URL
    assertTrue(result.contains("[Image: diagram]"));
    assertFalse(result.contains("data:image"));
    assertFalse(result.contains("base64"));
  }

  @Test
  public void testSanitizeMarkdownBase64ImageNoAltText() {
    String input = "Text ![](data:image/jpeg;base64,/9j/4AAQSkZJRg) more text";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    // Should remove image completely when no alt text
    assertEquals(result, "Text  more text");
  }

  @Test
  public void testSanitizeHtmlBase64Image() {
    String input = "Description <img src=\"data:image/png;base64,iVBORw\" width=\"500\"> text";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    assertTrue(result.contains("[Image]"));
    assertFalse(result.contains("<img"));
    assertFalse(result.contains("data:image"));
  }

  @Test
  public void testSanitizeMultipleImages() {
    String input = "![img1](data:image/png;base64,ABC) text ![img2](data:image/jpeg;base64,XYZ)";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    assertTrue(result.contains("[Image: img1]"));
    assertTrue(result.contains("[Image: img2]"));
    assertFalse(result.contains("base64"));
  }

  @Test
  public void testSanitizeMixedMarkdownAndHtml() {
    String input = "![md](data:image/png;base64,ABC) <img src=\"data:image/gif;base64,XYZ\">";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    assertTrue(result.contains("[Image: md]"));
    assertTrue(result.contains("[Image]"));
    assertFalse(result.contains("base64"));
  }

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
    assertEquals(SearchDocumentSanitizer.sanitizeForIndexing("   "), "");
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

    // Should complete in reasonable time (< 100ms for 1.4MB)
    assertTrue(
        duration < 100_000_000, "Sanitization took too long: " + duration / 1_000_000 + "ms");

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
  public void testSpecialCharactersInAltText() {
    String input = "![\"Special\" & <chars>](data:image/png;base64,ABC)";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    assertTrue(result.contains("[Image: \"Special\" & <chars>]"));
    assertFalse(result.contains("base64"));
  }

  @Test
  public void testMultilineBase64() {
    String input = "Text\n![img](data:image/png;base64,\nABC\nXYZ)\nMore text";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    assertTrue(result.contains("[Image: img]"));
    assertFalse(result.contains("base64"));
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

    // Should complete in reasonable time (< 50ms for 300KB)
    assertTrue(duration < 50_000_000, "Sanitization took too long: " + duration / 1_000_000 + "ms");

    // Should remove all images
    assertFalse(result.contains("base64"));
    assertTrue(result.contains("[Image: img0]"));
    assertTrue(result.contains("[Image: img1]"));
    assertTrue(result.contains("[Image: img2]"));
    assertTrue(result.contains("Start"));
    assertTrue(result.contains("End"));
  }

  @Test
  public void testCaseInsensitiveHtmlTag() {
    String input = "Text <IMG SRC=\"data:image/png;base64,ABC\"> more";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    assertTrue(result.contains("[Image]"));
    assertFalse(result.contains("IMG"));
    assertFalse(result.contains("data:image"));
  }

  @Test
  public void testDifferentImageTypes() {
    String input =
        "![png](data:image/png;base64,ABC) "
            + "![jpg](data:image/jpeg;base64,DEF) "
            + "![gif](data:image/gif;base64,GHI) "
            + "![svg](data:image/svg+xml;base64,JKL)";

    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    assertTrue(result.contains("[Image: png]"));
    assertTrue(result.contains("[Image: jpg]"));
    assertTrue(result.contains("[Image: gif]"));
    assertTrue(result.contains("[Image: svg]"));
    assertFalse(result.contains("base64"));
  }

  @Test
  public void testPreserveTextAroundImages() {
    String input =
        "This is the introduction. "
            + "![diagram](data:image/png;base64,ABCDEFGH) "
            + "This is the conclusion.";

    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    assertTrue(result.contains("This is the introduction"));
    assertTrue(result.contains("[Image: diagram]"));
    assertTrue(result.contains("This is the conclusion"));
    assertFalse(result.contains("base64"));
  }

  @Test
  public void testEmptyBase64String() {
    String input = "![empty](data:image/png;base64,)";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    // Should handle empty base64 string
    assertFalse(result.contains("data:image"));
  }

  @Test
  public void testMarkdownWithAngleBrackets() {
    // Real format from production logs: ![](<data:image/png;base64,...>)
    String input =
        "Description with ![](<data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAE7w>) image";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    // Should remove the image (no alt text)
    assertEquals(result, "Description with  image");
    assertFalse(result.contains("data:image"));
    assertFalse(result.contains("base64"));
  }

  @Test
  public void testMarkdownWithAngleBracketsAndAltText() {
    String input = "Text ![diagram](<data:image/png;base64,iVBORw0KGgoAAAA>) more text";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    // Should preserve alt text
    assertTrue(result.contains("[Image: diagram]"));
    assertFalse(result.contains("data:image"));
    assertFalse(result.contains("base64"));
  }

  @Test
  public void testMultipleImagesWithMixedBracketFormats() {
    String input = "![img1](data:image/png;base64,ABC) and ![img2](<data:image/jpeg;base64,XYZ>)";
    String result = SearchDocumentSanitizer.sanitizeForIndexing(input);

    // Both formats should be handled
    assertTrue(result.contains("[Image: img1]"));
    assertTrue(result.contains("[Image: img2]"));
    assertFalse(result.contains("base64"));
  }

  @Test
  public void testContainsBase64ImagesWithAngleBrackets() {
    assertTrue(
        SearchDocumentSanitizer.containsBase64Images("![](<data:image/png;base64,iVBORw0KGgo>)"));
    assertTrue(
        SearchDocumentSanitizer.containsBase64Images("![alt](<data:image/jpeg;base64,/9j/4AAQ>)"));
  }
}
