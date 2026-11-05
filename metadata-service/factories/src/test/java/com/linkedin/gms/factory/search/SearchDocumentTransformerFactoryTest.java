package com.linkedin.gms.factory.search;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.gms.factory.ProductionConfigContextRunner;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.testng.annotations.Test;

/**
 * Loads {@link SearchDocumentTransformerFactory} with the production {@code application.yaml} and
 * checks that the document size limits, the oversized structured-property switch and the Search V3
 * business attribute switch reach the {@code searchDocumentTransformer}, by transforming real
 * aspects with it.
 */
public class SearchDocumentTransformerFactoryTest {

  private static final EntityRegistry ENTITY_REGISTRY =
      TestOperationContexts.defaultEntityRegistry();
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");
  private static final Urn PROPERTY_URN = UrnUtils.getUrn("urn:li:structuredProperty:my_property");

  private static ApplicationContextRunner runner() {
    return ProductionConfigContextRunner.create()
        .withUserConfiguration(SearchDocumentTransformerFactory.class);
  }

  @Test
  public void testProductionDefaultsKeepModestCustomProperties() {
    runner()
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              assertThat(customProperties(context, 3, 10))
                  .containsExactlyInAnyOrder(
                      "key0=vvvvvvvvvv", "key1=vvvvvvvvvv", "key2=vvvvvvvvvv");
            });
  }

  @Test
  public void testDocumentSizeLimitsAreApplied() {
    runner()
        .withPropertyValues(
            "elasticsearch.index.maxArrayLength=2", "elasticsearch.index.maxValueLength=5")
        .run(
            context ->
                // Values over maxValueLength are dropped; customProperties is indexed as an array,
                // capped at maxArrayLength.
                assertThat(customProperties(context, 3, 10)).isEmpty());

    runner()
        .withPropertyValues(
            "elasticsearch.index.maxArrayLength=2", "elasticsearch.index.maxValueLength=20")
        .run(
            context ->
                assertThat(customProperties(context, 3, 10))
                    .hasSize(2)
                    .allMatch(entry -> entry.endsWith("=vvvvvvvvvv")));
  }

  @Test
  public void testOversizedStructuredPropertiesAreIndexedByDefault() {
    runner()
        .withPropertyValues("structuredProperties.keywordMaxLength=16")
        .run(
            context ->
                assertThat(structuredPropertyField(context, "a".repeat(17)).get(0).asText())
                    .isEqualTo("a".repeat(17)));
  }

  @Test
  public void testDropOversizedStructuredPropertiesUsesTheConfiguredLimit() {
    runner()
        .withPropertyValues(
            "structuredProperties.dropOversizedKeywordValuesFromIndex=true",
            "structuredProperties.keywordMaxLength=16")
        .run(
            context -> {
              assertThat(structuredPropertyField(context, "a".repeat(17)).isNull()).isTrue();
              assertThat(structuredPropertyField(context, "a".repeat(16)).get(0).asText())
                  .isEqualTo("a".repeat(16));
            });
  }

  @Test
  public void testNonPositiveKeywordMaxLengthFallsBackToTheElasticsearchLimit() {
    runner()
        .withPropertyValues(
            "structuredProperties.dropOversizedKeywordValuesFromIndex=true",
            "structuredProperties.keywordMaxLength=0")
        .run(
            context -> {
              String atLimit = "a".repeat(ESUtils.KEYWORD_MAXLENGTH);
              assertThat(structuredPropertyField(context, atLimit).get(0).asText())
                  .isEqualTo(atLimit);
              assertThat(structuredPropertyField(context, atLimit + "a").isNull()).isTrue();
            });
  }

  @Test
  public void testV3BusinessAttributeRefStaysAUrnByDefault() throws Exception {
    runner()
        .run(
            context -> {
              AspectRetriever aspectRetriever = businessAttributeRetriever();
              JsonNode ref = businessAttributeRef(context, aspectRetriever);
              assertThat(ref.asText()).isEqualTo("urn:li:businessAttribute:my_attribute");
              verify(aspectRetriever, never()).getLatestAspectObject(any(), any(), anyString());
            });
  }

  @Test
  public void testV3BusinessAttributeExpansionSwitchExpandsTheRef() {
    runner()
        .withPropertyValues("elasticsearch.search.v3.businessAttributeExpansionEnabled=true")
        .run(
            context -> {
              AspectRetriever aspectRetriever = businessAttributeRetriever();
              JsonNode ref = businessAttributeRef(context, aspectRetriever);
              assertThat(ref.isObject()).isTrue();
              verify(aspectRetriever, atLeastOnce())
                  .getLatestAspectObject(any(), any(), anyString());
            });
  }

  private static SearchDocumentTransformer transformer(ApplicationContext context) {
    return context.getBean("searchDocumentTransformer", SearchDocumentTransformer.class);
  }

  private static List<String> customProperties(
      ApplicationContext context, int keyCount, int valueLength) throws Exception {
    Map<String, String> properties = new LinkedHashMap<>();
    for (int i = 0; i < keyCount; i++) {
      properties.put("key" + i, "v".repeat(valueLength));
    }
    DatasetProperties aspect =
        new DatasetProperties().setCustomProperties(new StringMap(properties));
    ObjectNode document =
        transformer(context)
            .transformAspect(
                TestOperationContexts.systemContextNoSearchAuthorization(ENTITY_REGISTRY),
                DATASET_URN,
                aspect,
                ENTITY_REGISTRY
                    .getEntitySpec(DATASET_ENTITY_NAME)
                    .getAspectSpec(DATASET_PROPERTIES_ASPECT_NAME),
                false,
                AuditStampUtils.createDefaultAuditStamp())
            .orElseThrow();
    JsonNode field = document.get("customProperties");
    return field == null
        ? List.of()
        : StreamSupport.stream(field.spliterator(), false).map(JsonNode::asText).toList();
  }

  private static JsonNode structuredPropertyField(ApplicationContext context, String value)
      throws Exception {
    StructuredPropertyValueAssignment assignment =
        new StructuredPropertyValueAssignment()
            .setPropertyUrn(PROPERTY_URN)
            .setValues(new PrimitivePropertyValueArray(PrimitivePropertyValue.create(value)));
    StructuredProperties aspect =
        new StructuredProperties()
            .setProperties(new StructuredPropertyValueAssignmentArray(assignment));

    DataMap definition = new DataMap();
    definition.put("qualifiedName", "my_property");
    definition.put("valueType", "urn:li:dataType:datahub.string");
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getLatestAspectObjects(
            any(OperationFingerprint.class), anySet(), anySet()))
        .thenReturn(
            Map.of(
                PROPERTY_URN,
                Map.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME, new Aspect(definition))));

    return transformer(context)
        .transformAspect(
            operationContext(aspectRetriever),
            DATASET_URN,
            aspect,
            ENTITY_REGISTRY
                .getEntitySpec(DATASET_ENTITY_NAME)
                .getAspectSpec(STRUCTURED_PROPERTIES_ASPECT_NAME),
            false,
            AuditStampUtils.createDefaultAuditStamp())
        .orElseThrow()
        .get("structuredProperties.my_property");
  }

  private static AspectRetriever businessAttributeRetriever() {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(ENTITY_REGISTRY);
    when(aspectRetriever.getLatestAspectObject(any(), any(), anyString()))
        .thenReturn(new Aspect(new DataMap()));
    return aspectRetriever;
  }

  private static JsonNode businessAttributeRef(
      ApplicationContext context, AspectRetriever aspectRetriever) throws Exception {
    ObjectNode document = JsonNodeFactory.instance.objectNode();
    SearchDocumentTransformer.beginV3Projection();
    try {
      transformer(context)
          .setSearchableRefValue(
              operationContext(aspectRetriever),
              ENTITY_REGISTRY
                  .getEntitySpec("schemaField")
                  .getAspectSpec("businessAttributes")
                  .getSearchableRefFieldSpecs()
                  .get(0),
              List.of(UrnUtils.getUrn("urn:li:businessAttribute:my_attribute")),
              document,
              false,
              AuditStampUtils.createDefaultAuditStamp());
    } finally {
      SearchDocumentTransformer.endV3Projection();
    }
    return document.get("businessAttributeRef");
  }

  private static OperationContext operationContext(AspectRetriever aspectRetriever) {
    return TestOperationContexts.systemContextNoSearchAuthorization(
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(
                TestOperationContexts.emptyActiveUsersAspectRetriever(() -> ENTITY_REGISTRY))
            .graphRetriever(mock(GraphRetriever.class))
            .searchRetriever(mock(SearchRetriever.class))
            .build());
  }
}
