package com.linkedin.datahub.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.models.PagedCollection;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.SearchResultMetadata;
import com.linkedin.restli.common.CollectionResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nonnull;


public class RestliUtil {

  public static final ObjectMapper OM = new ObjectMapper();

  private RestliUtil() {
  }

  // convert restli collection response to PagedCollection
  @Nonnull
  public static <T extends RecordTemplate> PagedCollection<T> toPagedCollection(
      @Nonnull CollectionResponse<T> response) {
    PagedCollection<T> result = new PagedCollection<>();
    result.setElements(response.getElements());
    result.setStart(response.getPaging().getStart());
    result.setCount(response.getPaging().getCount());
    result.setTotal(response.getPaging().getTotal());
    return result;
  }

  // convert restli RecordTemplate to JsonNode
  @Nonnull
  public static <T extends RecordTemplate> JsonNode toJsonNode(@Nonnull T record) throws IOException {
    String jsonString = RecordUtils.toJsonString(record);
    return OM.readTree(jsonString);
  }

  // convert restli collection response to JsonNode
  @Nonnull
  public static <T extends RecordTemplate> JsonNode collectionResponseToJsonNode(
      @Nonnull CollectionResponse<T> response) throws IOException {
    final ObjectNode node = OM.createObjectNode();
    node.set("elements", collectionToArrayNode(response.getElements()));
    node.put("start", response.getPaging().getStart());
    node.put("count", response.getPaging().getCount());
    node.put("total", response.getPaging().getTotal());
    return node;
  }

  // Converts a collection to Json ArrayNode
  @Nonnull
  public static <T extends RecordTemplate> ArrayNode collectionToArrayNode(@Nonnull Collection<T> elements)
      throws IOException {
    final ArrayNode arrayNode = OM.createArrayNode();
    for (T element : elements) {
      arrayNode.add(toJsonNode(element));
    }
    return arrayNode;
  }

  // Converts a collection to Json ArrayNode. For each element in the collection, it adds the corresponding urn
  @Nonnull
  public static <T extends RecordTemplate> ArrayNode collectionToArrayNode(@Nonnull Collection<T> elements, @Nonnull UrnArray urns)
          throws IOException {
    if (elements.size() != urns.size()) {
      throw new RuntimeException("Collection size and urn size should match");
    }
    final ArrayNode arrayNode = OM.createArrayNode();
    final Iterator<Urn> urnIterator = urns.iterator();
    for (T element : elements) {
      final ObjectNode node = (ObjectNode) toJsonNode(element);
      arrayNode.add(node.put("urn", urnIterator.next().toString()));
    }
    return arrayNode;
  }

  // Convert restli collection response with metadata to JsonNode
  @Nonnull
  public static <T extends RecordTemplate> JsonNode collectionResponseWithMetadataToJsonNode(
          @Nonnull CollectionResponse<T> response) throws IOException {

    final SearchResultMetadata searchResultMetadata = new SearchResultMetadata(response.getMetadataRaw());
    final ObjectNode objectNode = (ObjectNode) collectionResponseToJsonNodeWithoutElements(response);
    objectNode.set("elements", collectionToArrayNode(response.getElements(), searchResultMetadata.getUrns()));

    if (response.getMetadataRaw() == null) {
      return objectNode;
    }

    // Use searchResultMetadatas field as a section in the final result json node
    final String name = SearchResultMetadata.fields().searchResultMetadatas().toString();
    final String fieldName = name.startsWith("/") ? name.substring(1) : name;

    objectNode.set(fieldName, toJsonNode(searchResultMetadata).get(fieldName));

    return objectNode;
  }

  // convert restli collection response to JsonNode without populating elements
  @Nonnull
  public static <T extends RecordTemplate> JsonNode collectionResponseToJsonNodeWithoutElements(
          @Nonnull CollectionResponse<T> response) throws IOException {
    final ObjectNode node = OM.createObjectNode();
    node.put("start", response.getPaging().getStart());
    node.put("count", response.getPaging().getCount());
    node.put("total", response.getPaging().getTotal());
    return node;
  }

  // Convert restli collection response with metadata to JsonNode
  @Nonnull
  public static <T extends RecordTemplate, Y extends RecordTemplate> JsonNode collectionResponseToJsonNode(
      @Nonnull CollectionResponse<T> response, @Nonnull Y metadata) throws IOException {

    JsonNode node = collectionResponseToJsonNode(response);

    ObjectNode objectNode = (ObjectNode) node;
    objectNode.set("metadata", toJsonNode(metadata));

    return objectNode;
  }

  // Converts restli map response to JsonNode
  @Nonnull
  public static <T extends RecordTemplate> JsonNode mapResponseToJsonNode(@Nonnull Map<?, T> map) throws IOException {
    ObjectNode node = OM.createObjectNode();
    for (Map.Entry<?, T> entry : map.entrySet()) {
      node.set(entry.getKey().toString(), toJsonNode(entry.getValue()));
    }
    return node;
  }

  // Converts a string collection to Json ArrayNode
  @Nonnull
  public static ArrayNode stringCollectionToArrayNode(@Nonnull Collection<String> elements) {
    final ArrayNode arrayNode = OM.createArrayNode();
    elements.forEach(s -> arrayNode.add(s));
    return arrayNode;
  }
}
