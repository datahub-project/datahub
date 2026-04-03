package com.linkedin.metadata.service.docimport;

import static org.testng.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.testng.annotations.Test;

public class TextExtractorsTest {

  @Test
  public void testGetExtension() {
    assertEquals(TextExtractors.getExtension("file.md"), ".md");
    assertEquals(TextExtractors.getExtension("file.TXT"), ".txt");
    assertEquals(TextExtractors.getExtension("path/to/doc.pdf"), ".pdf");
    assertEquals(TextExtractors.getExtension("no-extension"), "");
    assertEquals(TextExtractors.getExtension(".hidden"), ".hidden");
  }

  @Test
  public void testIsSupported() {
    assertTrue(TextExtractors.isSupported(".md"));
    assertTrue(TextExtractors.isSupported(".MD"));
    assertTrue(TextExtractors.isSupported(".txt"));
    assertTrue(TextExtractors.isSupported(".html"));
    assertTrue(TextExtractors.isSupported(".docx"));
    assertTrue(TextExtractors.isSupported(".pdf"));
    assertFalse(TextExtractors.isSupported(".exe"));
    assertFalse(TextExtractors.isSupported(".png"));
  }

  @Test
  public void testGetSupportedExtensions() {
    List<String> exts = TextExtractors.getSupportedExtensions();
    assertTrue(exts.contains(".md"));
    assertTrue(exts.contains(".txt"));
    assertTrue(exts.contains(".html"));
    assertTrue(exts.contains(".docx"));
    assertTrue(exts.contains(".pdf"));
    // Should be sorted
    assertEquals(exts, exts.stream().sorted().collect(java.util.stream.Collectors.toList()));
  }

  @Test
  public void testExtract_plainText() {
    byte[] content = "Hello world".getBytes(StandardCharsets.UTF_8);
    assertEquals(TextExtractors.extract(content, "test.txt"), "Hello world");
    assertEquals(TextExtractors.extract(content, "test.md"), "Hello world");
    assertEquals(TextExtractors.extract(content, "test.json"), "Hello world");
    assertEquals(TextExtractors.extract(content, "test.yaml"), "Hello world");
    assertEquals(TextExtractors.extract(content, "test.csv"), "Hello world");
    assertEquals(TextExtractors.extract(content, "test.rst"), "Hello world");
  }

  @Test
  public void testExtract_unsupportedFormat() {
    byte[] content = "data".getBytes(StandardCharsets.UTF_8);
    assertNull(TextExtractors.extract(content, "image.png"));
  }

  @Test
  public void testExtract_emptyContent() {
    byte[] content = "".getBytes(StandardCharsets.UTF_8);
    assertNull(TextExtractors.extract(content, "empty.txt"));
  }

  @Test
  public void testExtract_whitespaceOnlyContent() {
    byte[] content = "   \n\n  ".getBytes(StandardCharsets.UTF_8);
    assertNull(TextExtractors.extract(content, "blank.txt"));
  }

  @Test
  public void testExtractHtml_stripsTagsAndScripts() {
    String html =
        "<html><head><script>alert('xss')</script></head>"
            + "<body><h1>Title</h1><p>Hello <b>world</b></p></body></html>";
    byte[] content = html.getBytes(StandardCharsets.UTF_8);
    String result = TextExtractors.extract(content, "page.html");
    assertNotNull(result);
    assertTrue(result.contains("Title"));
    assertTrue(result.contains("Hello"));
    assertTrue(result.contains("world"));
    assertFalse(result.contains("<script>"));
    assertFalse(result.contains("<b>"));
    assertFalse(result.contains("alert"));
  }

  @Test
  public void testExtractHtml_decodesEntities() {
    String html = "<p>A &amp; B &lt; C</p>";
    byte[] content = html.getBytes(StandardCharsets.UTF_8);
    String result = TextExtractors.extract(content, "entities.html");
    assertNotNull(result);
    assertTrue(result.contains("A & B < C"));
  }

  @Test
  public void testTitleFromFilename_simple() {
    assertEquals(TextExtractors.titleFromFilename("setup.md"), "Setup");
  }

  @Test
  public void testTitleFromFilename_withPath() {
    assertEquals(
        TextExtractors.titleFromFilename("docs/guides/getting-started.md"), "Getting Started");
  }

  @Test
  public void testTitleFromFilename_underscores() {
    assertEquals(TextExtractors.titleFromFilename("my_cool_doc.txt"), "My Cool Doc");
  }

  @Test
  public void testTitleFromFilename_noExtension() {
    assertEquals(TextExtractors.titleFromFilename("README"), "Readme");
  }

  @Test
  public void testTitleFromFilename_dirPath() {
    assertEquals(TextExtractors.titleFromFilename("guides/advanced"), "Advanced");
  }
}
