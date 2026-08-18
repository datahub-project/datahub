package io.datahubproject.openlineage.customfacet;

import static io.datahubproject.openlineage.converter.OpenLineageToDataHub.JOB_CALL_SITE_KEY;
import static io.datahubproject.openlineage.converter.OpenLineageToDataHub.JOB_DESCRIPTION_KEY;
import static io.datahubproject.openlineage.converter.OpenLineageToDataHub.JOB_GROUP_KEY;
import static io.datahubproject.openlineage.converter.OpenLineageToDataHub.JOB_ID_KEY;
import static io.datahubproject.openlineage.converter.OpenLineageToDataHub.OPENLINEAGE_SPARK_VERSION_KEY;
import static io.datahubproject.openlineage.converter.OpenLineageToDataHub.SPARK_LOGICAL_PLAN_KEY;
import static io.datahubproject.openlineage.converter.OpenLineageToDataHub.SPARK_VERSION_KEY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.data.template.StringMap;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetCatalog.AttachmentPoint;
import io.datahubproject.openlineage.customfacet.CompatibilityFacetCatalog.SupportStatus;
import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONException;

@Slf4j
public final class CustomRunFacetProcessor {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public CustomRunFacetContributions process(OpenLineage.RunFacets facets) {
    if (facets == null) {
      return CustomRunFacetContributions.empty();
    }
    Map<String, OpenLineage.RunFacet> runFacets;
    try {
      runFacets = facets.getAdditionalProperties();
    } catch (RuntimeException exception) {
      log.debug("Ignoring malformed OpenLineage run facets container");
      return CustomRunFacetContributions.empty();
    }
    if (runFacets == null) {
      return CustomRunFacetContributions.empty();
    }

    StringMap flowProperties = new StringMap();
    StringMap jobProperties = new StringMap();
    GlobalTags flowTags = null;

    for (CompatibilityFacetContract contract : CompatibilityFacetCatalog.contracts()) {
      OpenLineage.RunFacet facet = runFacets.get(contract.key());
      JsonNode payload = compatiblePayload(contract, facet);
      if (payload == null) {
        continue;
      }
      if (contract.status() == SupportStatus.DEPRECATED) {
        log.debug("Processing deprecated OpenLineage compatibility facet '{}'", contract.key());
      }
      switch (contract.key()) {
        case "airflow":
          copyScalarProperties(payload.get("dag"), flowProperties, contract.key());
          copyScalarProperties(payload.get("task"), jobProperties, contract.key());
          flowTags = airflowTags(payload.path("dag").get("tags"));
          break;
        case "spark_jobDetails":
          copySparkJobDetails(payload, flowProperties, contract.key());
          copySparkJobDetails(payload, jobProperties, contract.key());
          break;
        case "spark_properties":
          copyScalarProperties(payload.get("properties"), flowProperties, contract.key());
          copyScalarProperties(payload.get("properties"), jobProperties, contract.key());
          break;
        case "spark.logicalPlan":
          copyLogicalPlan(payload, flowProperties, contract.key());
          break;
        case "spark_version":
          copySparkVersion(payload, flowProperties, contract.key());
          copySparkVersion(payload, jobProperties, contract.key());
          break;
        case "unknownSourceAttribute":
          copyUnknownSourceAttributes(payload, jobProperties, contract.key());
          break;
        default:
          break;
      }
    }

    return new CustomRunFacetContributions(flowProperties, jobProperties, flowTags);
  }

  private static JsonNode compatiblePayload(
      CompatibilityFacetContract contract, OpenLineage.RunFacet facet) {
    if (facet == null) {
      return null;
    }
    try {
      URI producer = facet.get_producer();
      URI schemaUrl = facet.get_schemaURL();
      if (!contract.matches(AttachmentPoint.RUN, schemaUrl, producer)) {
        return null;
      }
      JsonNode payload = OBJECT_MAPPER.valueToTree(facet);
      return hasExpectedShape(contract.key(), payload) ? payload : null;
    } catch (RuntimeException exception) {
      return null;
    }
  }

  private static boolean hasExpectedShape(String key, JsonNode payload) {
    if (!payload.isObject()) {
      return false;
    }
    switch (key) {
      case "airflow":
        JsonNode dag = payload.get("dag");
        JsonNode task = payload.get("task");
        return (dag != null || task != null)
            && isObjectOrMissing(dag)
            && isObjectOrMissing(task)
            && (dag == null || isTextualOrMissing(dag.get("tags")));
      case "spark_jobDetails":
        return payload.path("jobId").isIntegralNumber()
            && isTextualOrMissing(payload.get("jobDescription"))
            && isTextualOrMissing(payload.get("jobGroup"))
            && isTextualOrMissing(payload.get("jobCallSite"));
      case "spark_properties":
        return isScalarProperties(payload.get("properties"));
      case "spark.logicalPlan":
        return payload.path("plan").isArray();
      case "spark_version":
        return payload.path("spark-version").isTextual()
            && isTextualOrMissing(payload.get("openlineage-spark-version"));
      case "unknownSourceAttribute":
        return hasExpectedUnknownItems(payload.get("unknownItems"));
      default:
        return false;
    }
  }

  private static boolean isObjectOrMissing(JsonNode node) {
    return node == null || node.isObject();
  }

  private static boolean isScalarProperties(JsonNode node) {
    if (node == null || !node.isObject()) {
      return false;
    }
    Iterator<JsonNode> values = node.elements();
    while (values.hasNext()) {
      if (!isScalar(values.next())) {
        return false;
      }
    }
    return true;
  }

  private static boolean isTextualOrMissing(JsonNode node) {
    return node == null || node.isTextual();
  }

  private static boolean hasExpectedUnknownItems(JsonNode unknownItems) {
    if (unknownItems == null || !unknownItems.isArray()) {
      return false;
    }
    for (JsonNode item : unknownItems) {
      if (!item.isObject()
          || !item.path("type").isTextual()
          || !item.path("name").isTextual()
          || (item.has("properties") && !item.path("properties").isObject())) {
        return false;
      }
    }
    return true;
  }

  private static void copySparkJobDetails(
      JsonNode payload, StringMap destination, String facetKey) {
    putScalar(payload, "jobId", JOB_ID_KEY, destination, facetKey);
    putScalar(payload, "jobDescription", JOB_DESCRIPTION_KEY, destination, facetKey);
    putScalar(payload, "jobGroup", JOB_GROUP_KEY, destination, facetKey);
    putScalar(payload, "jobCallSite", JOB_CALL_SITE_KEY, destination, facetKey);
  }

  private static void copySparkVersion(JsonNode payload, StringMap destination, String facetKey) {
    putScalar(payload, "spark-version", SPARK_VERSION_KEY, destination, facetKey);
    putScalar(
        payload, "openlineage-spark-version", OPENLINEAGE_SPARK_VERSION_KEY, destination, facetKey);
  }

  private static void putScalar(
      JsonNode source,
      String sourceKey,
      String destinationKey,
      StringMap destination,
      String facetKey) {
    JsonNode value = source.get(sourceKey);
    if (isScalar(value)) {
      putContribution(destination, destinationKey, value.asText(), facetKey);
    }
  }

  private static void copyScalarProperties(
      JsonNode source, StringMap destination, String facetKey) {
    if (source == null || !source.isObject()) {
      return;
    }
    List<Map.Entry<String, JsonNode>> entries = new ArrayList<>();
    Iterator<Map.Entry<String, JsonNode>> fields = source.fields();
    fields.forEachRemaining(entries::add);
    entries.sort(Comparator.comparing(Map.Entry::getKey));
    for (Map.Entry<String, JsonNode> entry : entries) {
      if (isScalar(entry.getValue())) {
        putContribution(destination, entry.getKey(), entry.getValue().asText(), facetKey);
      }
    }
  }

  private static void putContribution(
      StringMap destination, String property, String value, String facetKey) {
    if (destination.containsKey(property)) {
      log.debug(
          "Skipping colliding OpenLineage property '{}' from compatibility facet '{}'",
          property,
          facetKey);
      return;
    }
    destination.put(property, value);
  }

  private static boolean isScalar(JsonNode node) {
    return node != null && (node.isTextual() || node.isNumber() || node.isBoolean());
  }

  private static void copyLogicalPlan(JsonNode payload, StringMap destination, String facetKey) {
    JsonNode plan = payload.get("plan");
    if (plan == null || !plan.isArray()) {
      return;
    }
    ObjectNode logicalPlan = OBJECT_MAPPER.createObjectNode();
    logicalPlan.set("plan", plan);
    try {
      putContribution(
          destination,
          SPARK_LOGICAL_PLAN_KEY,
          OBJECT_MAPPER.writeValueAsString(logicalPlan),
          facetKey);
    } catch (JsonProcessingException exception) {
      // JsonNode serialization is expected to be total; a failure yields no contribution.
    }
  }

  private static void copyUnknownSourceAttributes(
      JsonNode payload, StringMap destination, String facetKey) {
    JsonNode unknownItems = payload.get("unknownItems");
    if (unknownItems == null || !unknownItems.isArray()) {
      return;
    }
    for (JsonNode item : unknownItems) {
      if (!item.isObject() || !item.path("type").isTextual() || !item.path("name").isTextual()) {
        continue;
      }
      copyScalarProperties(item, destination, facetKey);
      copyScalarProperties(item.get("properties"), destination, facetKey);
    }
  }

  private static GlobalTags airflowTags(JsonNode tagsNode) {
    if (tagsNode == null || !tagsNode.isTextual()) {
      return null;
    }
    try {
      JSONArray values = parseTags(tagsNode.textValue());
      LinkedHashSet<String> uniqueTags = new LinkedHashSet<>();
      for (int index = 0; index < values.length(); index++) {
        Object value = values.get(index);
        if (!(value instanceof String)) {
          return null;
        }
        uniqueTags.add((String) value);
      }
      return uniqueTags.isEmpty() ? null : generateTags(new ArrayList<>(uniqueTags));
    } catch (JSONException exception) {
      return null;
    }
  }

  private static GlobalTags generateTags(List<String> tags) {
    tags.sort(String::compareToIgnoreCase);
    TagAssociationArray associations = new TagAssociationArray();
    for (String tag : tags) {
      associations.add(new TagAssociation().setTag(new TagUrn(tag)));
    }
    return new GlobalTags().setTags(associations);
  }

  private static JSONArray parseTags(String value) {
    try {
      return new JSONArray(value);
    } catch (JSONException exception) {
      return new JSONArray(value.replace('\'', '"'));
    }
  }
}
