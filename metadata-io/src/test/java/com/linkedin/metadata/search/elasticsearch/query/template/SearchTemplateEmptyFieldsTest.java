package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_FULLTEXT_URN;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import com.linkedin.search.template.SearchFieldOverrides;
import com.linkedin.search.template.SearchTemplateDialect;
import com.linkedin.search.template.SearchTemplateProperties;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.ResolvedSearchTemplate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.opensearch.index.query.QueryBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * A template whose field section leaves no query-by-default field still renders a valid query.
 * OpenSearch reads an empty {@code fields} list as every field, so such a clause must not be sent.
 */
public class SearchTemplateEmptyFieldsTest {
  private static final Set<String> MULTI =
      Set.of("simple_query_string", "multi_match", "query_string");
  private static final Set<String> FIELD_KEYED =
      Set.of(
          "term",
          "match",
          "match_phrase",
          "match_phrase_prefix",
          "prefix",
          "wildcard",
          "regexp",
          "fuzzy");

  SearchTemplateFixtures.Harness harness;

  @BeforeMethod
  public void setup() {
    harness = new SearchTemplateFixtures.Harness();
    harness.setup();
    harness.installCatalog();
    harness.installShippedTemplates();
  }

  @Test
  public void testShippedTemplatesOmitEmptyFieldClauses() throws Exception {
    Map<String, String> queryByTemplate = new LinkedHashMap<>();
    queryByTemplate.put(SEARCH_TEMPLATE_FULLTEXT_URN, "orders");
    queryByTemplate.put("urn:li:searchTemplate:light", "orders");
    queryByTemplate.put("urn:li:searchTemplate:identity", "urn:li:corpuser:datahub");
    assertTemplates(queryByTemplate);
  }

  void assertTemplates(Map<String, String> queryByTemplate) throws Exception {
    EntitySpec dataset = harness.registry.getEntitySpec("dataset");
    for (Map.Entry<String, String> entry : queryByTemplate.entrySet()) {
      String child = withoutFields(entry.getKey(), entry.getValue(), dataset);
      String rendered = render(child, entry.getValue(), dataset);
      if (entry.getKey().contains("fulltext")) {
        // The fulltext head clause is replaced only when the default list is empty, so this keeps
        // the child a valid fixture for the empty case.
        assertTrue(rendered.contains("match_none"), "child still has default fields: " + rendered);
      }
      assertNoEmptyFieldClause(rendered, entry);
    }
  }

  /** Stores a child of {@code parent} that removes every field the parent queries. */
  private String withoutFields(String parent, String query, EntitySpec spec) throws Exception {
    Set<String> queried = new TreeSet<>();
    collectFields(SearchTemplateFixtures.JSON.readTree(render(parent, query, spec)), queried);
    StringArray remove = new StringArray();
    for (String field : queried) {
      // Property paths are not logical names, and the property clauses come from the catalog.
      if (field.startsWith("structuredProperties.")) {
        continue;
      }
      remove.add(field);
      int dot = field.indexOf('.');
      if (dot > 0) {
        remove.add(field.substring(0, dot));
      }
    }
    SearchFieldOverrides fields = new SearchFieldOverrides();
    fields.setRemove(remove);
    SearchTemplateProperties child = new SearchTemplateProperties();
    child.setName("No fields");
    child.setDialect(SearchTemplateDialect.ELASTICSEARCH);
    child.setEditable(true);
    child.setParent(UrnUtils.getUrn(parent));
    child.setFields(fields);
    String urn = parent + "NoFields";
    harness.store(urn, child);
    return urn;
  }

  private String render(String urn, String query, EntitySpec spec) {
    OperationContext bound =
        harness.opContext.withResolvedSearchTemplate(
            new ResolvedSearchTemplate(urn, 1L, "fulltext", null));
    QueryBuilder rendered =
        new SearchV3QueryBuilder(harness.loader, new SearchConfiguration(), null)
            .render(bound, List.of(spec), query, true);
    return rendered.toString();
  }

  private static void collectFields(JsonNode node, Set<String> out) {
    if (node.isArray()) {
      node.forEach(child -> collectFields(child, out));
      return;
    }
    if (!node.isObject()) {
      return;
    }
    JsonNode fields = node.get("fields");
    if (fields != null && fields.isArray()) {
      for (JsonNode field : fields) {
        String name = field.asText();
        int caret = name.lastIndexOf('^');
        out.add(caret > 0 ? name.substring(0, caret) : name);
      }
    }
    node.forEach(child -> collectFields(child, out));
  }

  static void assertNoEmptyFieldClause(String rendered, Object label) throws Exception {
    JsonNode root = SearchTemplateFixtures.JSON.readTree(rendered);
    List<String> problems = new ArrayList<>();
    walk(root, problems);
    assertTrue(problems.isEmpty(), label + " " + problems + " in " + rendered);
    assertFalse(rendered.replace(" ", "").contains("\"fields\":[]"), label + " " + rendered);
  }

  private static void walk(JsonNode node, List<String> problems) {
    if (node.isArray()) {
      node.forEach(child -> walk(child, problems));
      return;
    }
    if (!node.isObject()) {
      return;
    }
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String type = entry.getKey();
      JsonNode body = entry.getValue();
      if (MULTI.contains(type) && body.isObject()) {
        JsonNode list = body.get("fields");
        boolean hasDefault = body.hasNonNull("default_field");
        if (!hasDefault && (list == null || !list.isArray() || list.isEmpty())) {
          problems.add(type + " without fields");
        }
      }
      if (FIELD_KEYED.contains(type) && body.isObject()) {
        body.fieldNames()
            .forEachRemaining(
                name -> {
                  if (name.isBlank()) {
                    problems.add(type + " with a blank field");
                  }
                });
      }
      walk(body, problems);
    }
  }
}
