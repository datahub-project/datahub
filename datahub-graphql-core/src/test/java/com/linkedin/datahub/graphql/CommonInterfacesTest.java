package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.DATASET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.GLOBAL_TAGS_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import graphql.ExecutionResult;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.Answers;
import org.testng.annotations.Test;

public class CommonInterfacesTest {

  private static final String DATASET = "urn:li:dataset:(urn:li:dataPlatform:hive,orders,PROD)";
  private static final String TAG = "urn:li:tag:pii";

  @Test
  public void testFieldsResolveThroughInterfaceFragments() throws Exception {
    String query =
        "{ entity(urn: \""
            + DATASET
            + "\") { ... on HasProperties { properties { name description"
            + " ... on HasExternalUrl { externalUrl } } }"
            + " ... on HasTags { tags { tags { tag { urn } } } } } }";

    ExecutionResult result = execute(query);

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    Map<String, Object> entity = get(result.getData(), "entity");
    assertEquals(
        get(entity, "properties"),
        Map.of(
            "name", "orders",
            "description", "All orders",
            "externalUrl", "https://example.com/orders"));
    assertEquals(get(entity, "tags", "tags"), List.of(Map.of("tag", Map.of("urn", TAG))));
  }

  private ExecutionResult execute(String query) throws Exception {
    QueryContext context = getMockAllowContext();
    EntityRegistry registry = context.getOperationContext().getEntityRegistry();
    Map<String, RecordTemplate> datasetAspects =
        Map.of(
            DATASET_PROPERTIES_ASPECT_NAME,
            new DatasetProperties()
                .setName("orders")
                .setDescription("All orders")
                .setExternalUrl(new Url("https://example.com/orders")),
            GLOBAL_TAGS_ASPECT_NAME,
            new GlobalTags()
                .setTags(
                    new TagAssociationArray(
                        new TagAssociation().setTag(TagUrn.createFromString(TAG)))));

    EntityClient entityClient =
        mock(
            EntityClient.class,
            invocation -> {
              if (!invocation.getMethod().getName().equals("batchGetV2")) {
                return Answers.RETURNS_DEFAULTS.answer(invocation);
              }
              Map<Urn, EntityResponse> responses = new HashMap<>();
              for (Object requested : (Set<?>) invocation.getArgument(2)) {
                Urn urn = (Urn) requested;
                EntityResponse response = keyOnlyResponse(registry, urn);
                if (urn.toString().equals(DATASET)) {
                  datasetAspects.forEach(
                      (name, aspect) ->
                          response
                              .getAspects()
                              .put(
                                  name,
                                  new EnvelopedAspect()
                                      .setName(name)
                                      .setValue(new Aspect(aspect.data()))));
                }
                responses.put(urn, response);
              }
              return responses;
            });
    return MockedGmsGraphQLEngine.execute(query, context, entityClient);
  }

  private static EntityResponse keyOnlyResponse(EntityRegistry registry, Urn urn) {
    AspectSpec keySpec = registry.getEntitySpec(urn.getEntityType()).getKeyAspectSpec();
    EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    aspects.put(
        keySpec.getName(),
        new EnvelopedAspect()
            .setName(keySpec.getName())
            .setValue(new Aspect(EntityKeyUtils.convertUrnToEntityKey(urn, keySpec).data())));
    return new EntityResponse().setUrn(urn).setEntityName(urn.getEntityType()).setAspects(aspects);
  }

  @SuppressWarnings("unchecked")
  private static <T> T get(Object node, String... path) {
    Object current = node;
    for (String key : path) {
      current = ((Map<String, Object>) current).get(key);
    }
    return (T) current;
  }
}
