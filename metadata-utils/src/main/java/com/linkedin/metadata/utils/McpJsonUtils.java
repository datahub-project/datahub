package com.linkedin.metadata.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for parsing MetadataChangeProposal objects from JSON files.
 *
 * <p>Supports a simplified JSON format designed for human-readable sample data files:
 *
 * <pre>
 * [
 *   {
 *     "entityType": "dataset",
 *     "entityUrn": "urn:li:dataset:...",
 *     "changeType": "UPSERT",
 *     "aspectName": "datasetProperties",
 *     "aspect": {
 *       "json": { ... actual aspect content ... }
 *     },
 *     "systemMetadata": {
 *       "properties": { "key": "value" }
 *     }
 *   }
 * ]
 * </pre>
 */
@Slf4j
public class McpJsonUtils {

  private McpJsonUtils() {}

  /**
   * Parse a list of MCPs from a JSON array input stream.
   *
   * @param mapper ObjectMapper for JSON parsing
   * @param inputStream Input stream containing JSON array of MCPs
   * @return List of parsed MetadataChangeProposal objects
   * @throws IOException If JSON parsing fails
   */
  @Nonnull
  public static List<MetadataChangeProposal> parseMcpsFromJsonStream(
      @Nonnull ObjectMapper mapper, @Nonnull InputStream inputStream) throws IOException {
    return parseMcpsFromJsonStream(mapper, inputStream, -1);
  }

  /**
   * Parse a list of MCPs from a JSON array input stream with error limit.
   *
   * @param mapper ObjectMapper for JSON parsing
   * @param inputStream Input stream containing JSON array of MCPs
   * @param maxErrorsToLog Maximum number of parse errors to log (-1 for unlimited)
   * @return List of successfully parsed MetadataChangeProposal objects
   * @throws IOException If JSON parsing fails at the array level
   */
  @Nonnull
  public static List<MetadataChangeProposal> parseMcpsFromJsonStream(
      @Nonnull ObjectMapper mapper, @Nonnull InputStream inputStream, int maxErrorsToLog)
      throws IOException {

    JsonNode mcpArray = mapper.readTree(inputStream);

    if (!mcpArray.isArray()) {
      throw new IllegalArgumentException(
          String.format("Expected JSON array of MCPs but found %s", mcpArray.getNodeType()));
    }

    List<MetadataChangeProposal> proposals = new ArrayList<>();
    int errorCount = 0;

    for (int i = 0; i < mcpArray.size(); i++) {
      try {
        MetadataChangeProposal mcp = parseMcpFromJsonNode(mapper, mcpArray.get(i));
        proposals.add(mcp);
      } catch (Exception e) {
        errorCount++;
        if (maxErrorsToLog < 0 || errorCount <= maxErrorsToLog) {
          log.warn("Failed to parse MCP at index {}: {}", i, e.getMessage());
        }
      }
    }

    if (errorCount > 0) {
      log.info("Parsed {} MCPs with {} errors", proposals.size(), errorCount);
    }

    return proposals;
  }

  /**
   * Parse a single MCP from a JSON node.
   *
   * @param mapper ObjectMapper for JSON serialization
   * @param node JSON node containing MCP data
   * @return Parsed MetadataChangeProposal
   * @throws URISyntaxException If entityUrn is invalid
   * @throws IOException If JSON serialization fails
   */
  @Nonnull
  public static MetadataChangeProposal parseMcpFromJsonNode(
      @Nonnull ObjectMapper mapper, @Nonnull JsonNode node) throws URISyntaxException, IOException {

    MetadataChangeProposal mcp = new MetadataChangeProposal();

    // Required fields
    mcp.setEntityType(getRequiredString(node, "entityType"));
    mcp.setEntityUrn(Urn.createFromString(getRequiredString(node, "entityUrn")));
    mcp.setChangeType(ChangeType.valueOf(getRequiredString(node, "changeType")));
    mcp.setAspectName(getRequiredString(node, "aspectName"));

    // Handle aspect - supports {"json": {...}} format
    JsonNode aspectNode = node.get("aspect");
    if (aspectNode != null) {
      JsonNode jsonValue = aspectNode.get("json");
      if (jsonValue != null) {
        String aspectJson = mapper.writeValueAsString(jsonValue);
        GenericAspect genericAspect = new GenericAspect();
        genericAspect.setContentType("application/json");
        genericAspect.setValue(ByteString.unsafeWrap(aspectJson.getBytes(StandardCharsets.UTF_8)));
        mcp.setAspect(genericAspect);
      }
    }

    // Handle systemMetadata
    JsonNode systemMetadataNode = node.get("systemMetadata");
    if (systemMetadataNode != null) {
      SystemMetadata systemMetadata = new SystemMetadata();
      JsonNode propertiesNode = systemMetadataNode.get("properties");
      if (propertiesNode != null && propertiesNode.isObject()) {
        StringMap properties = new StringMap();
        propertiesNode
            .fields()
            .forEachRemaining(entry -> properties.put(entry.getKey(), entry.getValue().asText()));
        systemMetadata.setProperties(properties);
      }
      mcp.setSystemMetadata(systemMetadata);
    }

    return mcp;
  }

  private static String getRequiredString(JsonNode node, String fieldName) {
    JsonNode fieldNode = node.get(fieldName);
    if (fieldNode == null || fieldNode.isNull()) {
      throw new IllegalArgumentException("Missing required field: " + fieldName);
    }
    return fieldNode.asText();
  }
}
