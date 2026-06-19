package com.linkedin.metadata.service.docimport;

import static org.testng.Assert.*;

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
