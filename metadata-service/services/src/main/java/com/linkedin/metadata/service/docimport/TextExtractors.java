package com.linkedin.metadata.service.docimport;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import lombok.extern.slf4j.Slf4j;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Text extraction utilities for various file formats. Uses Java stdlib wherever possible to
 * minimize dependencies.
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
    m.put(".docx", TextExtractors::extractDocx);
    m.put(".pdf", TextExtractors::extractPdf);
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
  private static final Set<String> PDF_EXTENSION = Set.of(".pdf");

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
    // Strip non-content blocks
    html = STRIP_BLOCKS.matcher(html).replaceAll("");
    // Convert block elements to newlines
    html = html.replaceAll("(?i)<(br|p|div|h[1-6]|li|tr)[^>]*>", "\n");
    // Strip remaining HTML tags
    html = HTML_TAGS.matcher(html).replaceAll("");
    // Decode common HTML entities
    html = decodeHtmlEntities(html);
    // Collapse excessive whitespace
    html = WHITESPACE_RUNS.matcher(html.trim()).replaceAll("\n\n");
    return html;
  }

  /**
   * Extracts text from DOCX files by parsing the ZIP-embedded word/document.xml. Uses only Java
   * stdlib (java.util.zip + javax.xml.parsers). Extracts text from w:t elements in the document
   * body.
   */
  static String extractDocx(byte[] content, String filename) throws Exception {
    byte[] documentXml = extractEntryFromZip(content, "word/document.xml");
    if (documentXml == null) {
      throw new IOException("No word/document.xml found in DOCX file: " + filename);
    }

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    // Disable external entities to prevent XXE
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);

    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(new java.io.ByteArrayInputStream(documentXml));

    StringBuilder sb = new StringBuilder();
    extractTextFromXml(doc.getDocumentElement(), sb);
    return sb.toString().trim();
  }

  /**
   * Extracts text from PDF files using Apache PDFBox. PDFBox is loaded reflectively to keep it as
   * an optional dependency — if it's not on the classpath, PDF extraction returns null with a
   * warning.
   */
  static String extractPdf(byte[] content, String filename) throws Exception {
    try {
      Class<?> pdfDocClass = Class.forName("org.apache.pdfbox.pdmodel.PDDocument");
      Class<?> stripperClass = Class.forName("org.apache.pdfbox.text.PDFTextStripper");

      Object pdDoc = pdfDocClass.getMethod("load", byte[].class).invoke(null, (Object) content);
      try {
        Object stripper = stripperClass.getDeclaredConstructor().newInstance();
        return (String) stripperClass.getMethod("getText", pdfDocClass).invoke(stripper, pdDoc);
      } finally {
        pdfDocClass.getMethod("close").invoke(pdDoc);
      }
    } catch (ClassNotFoundException e) {
      log.warn(
          "PDF extraction requires Apache PDFBox on the classpath. "
              + "Add pdfbox dependency to enable PDF import.");
      return null;
    } catch (Exception e) {
      Throwable cause = e.getCause() != null ? e.getCause() : e;
      throw new IOException("Failed to extract text from PDF: " + cause.getMessage(), cause);
    }
  }

  // -- Helpers --

  @Nonnull
  static String getExtension(@Nonnull String filename) {
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
    // Replace dashes/underscores with spaces and title-case
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

  @Nullable
  private static byte[] extractEntryFromZip(byte[] zipContent, String entryName)
      throws IOException {
    try (ZipInputStream zis = new ZipInputStream(new java.io.ByteArrayInputStream(zipContent))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().equals(entryName)) {
          return zis.readAllBytes();
        }
      }
    }
    return null;
  }

  /**
   * Recursively extracts text content from XML w:t elements (Word text runs). Inserts newlines
   * between paragraphs (w:p elements).
   */
  private static void extractTextFromXml(Node node, StringBuilder sb) {
    if (node.getNodeType() == Node.ELEMENT_NODE) {
      String localName = node.getLocalName();
      if ("t".equals(localName)) {
        sb.append(node.getTextContent());
      } else if ("p".equals(localName)) {
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) != '\n') {
          sb.append('\n');
        }
      }
    }
    NodeList children = node.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      extractTextFromXml(children.item(i), sb);
    }
  }
}
