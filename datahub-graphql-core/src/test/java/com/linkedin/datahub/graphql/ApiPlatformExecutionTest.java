package com.linkedin.datahub.graphql;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataplatform.PlatformType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataPlatformInstanceKey;
import com.linkedin.metadata.key.DataPlatformKey;
import graphql.ExecutionResult;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.Answers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

/**
 * Runs the selection the web UI's search cards use against the real schema and wiring. Api
 * implements HasDataPlatformInstance, whose urn, type and instanceId are non-null, while an Api is
 * often on a platform without an instance. That must neither fail the query nor lose the platform.
 */
public class ApiPlatformExecutionTest {

  private static final String PLATFORM_URN = "urn:li:dataPlatform:openapi";
  private static final String INSTANCE_URN =
      "urn:li:dataPlatformInstance:(urn:li:dataPlatform:openapi,prod)";
  private static final String PLATFORM_ONLY_API = "urn:li:api:platform-only";
  private static final String INSTANCE_API = "urn:li:api:with-instance";

  private static final String SELECTION =
      "urn ... on Api { platform { urn name } }"
          + " ... on HasDataPlatformInstance {"
          + " dataPlatformInstance { urn type instanceId platform { urn } } }";

  @Test
  public void testPlatformOnlyApiKeepsItsPlatformAndHasNoInstance() throws Exception {
    final ExecutionResult result =
        execute("{ entity(urn: \"" + PLATFORM_ONLY_API + "\") { " + SELECTION + " } }");

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    final Map<String, Object> entity = entity(result);
    assertEquals(platform(entity).get("urn"), PLATFORM_URN);
    assertEquals(platform(entity).get("name"), "openapi");
    assertNull(entity.get("dataPlatformInstance"));
  }

  @Test
  public void testApiWithInstanceResolvesPlatformAndInstance() throws Exception {
    final ExecutionResult result =
        execute("{ entity(urn: \"" + INSTANCE_API + "\") { " + SELECTION + " } }");

    assertTrue(result.getErrors().isEmpty(), result.getErrors().toString());
    final Map<String, Object> entity = entity(result);
    assertEquals(platform(entity).get("urn"), PLATFORM_URN);
    @SuppressWarnings("unchecked")
    final Map<String, Object> instance = (Map<String, Object>) entity.get("dataPlatformInstance");
    assertEquals(instance.get("urn"), INSTANCE_URN);
    assertEquals(instance.get("type"), "DATA_PLATFORM_INSTANCE");
    assertEquals(instance.get("instanceId"), "prod");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> entity(ExecutionResult result) {
    return (Map<String, Object>) ((Map<String, Object>) result.getData()).get("entity");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> platform(Map<String, Object> entity) {
    return (Map<String, Object>) entity.get("platform");
  }

  private ExecutionResult execute(String query) throws Exception {
    return MockedGmsGraphQLEngine.execute(
        query, getMockAllowContext(), mock(EntityClient.class, backend()));
  }

  /** Two Apis, the platform and the instance, each with only the aspects the test needs. */
  private static Answer<Object> backend() {
    return invocation -> {
      if (!invocation.getMethod().getName().equals("batchGetV2")) {
        return Answers.RETURNS_DEFAULTS.answer(invocation);
      }
      final Map<Urn, EntityResponse> responses = new HashMap<>();
      for (Object requested : (Set<?>) invocation.getArgument(2)) {
        final Urn urn = (Urn) requested;
        responses.put(urn, response(urn));
      }
      return responses;
    };
  }

  private static EntityResponse response(Urn urn) {
    final EnvelopedAspectMap aspects = new EnvelopedAspectMap();
    switch (urn.toString()) {
      case PLATFORM_ONLY_API:
        aspects.put(
            Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
            aspect(new DataPlatformInstance().setPlatform(UrnUtils.getUrn(PLATFORM_URN))));
        break;
      case INSTANCE_API:
        aspects.put(
            Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
            aspect(
                new DataPlatformInstance()
                    .setPlatform(UrnUtils.getUrn(PLATFORM_URN))
                    .setInstance(UrnUtils.getUrn(INSTANCE_URN))));
        break;
      case PLATFORM_URN:
        aspects.put(
            Constants.DATA_PLATFORM_KEY_ASPECT_NAME,
            aspect(new DataPlatformKey().setPlatformName("openapi")));
        aspects.put(
            Constants.DATA_PLATFORM_INFO_ASPECT_NAME,
            aspect(
                new DataPlatformInfo()
                    .setName("openapi")
                    .setType(PlatformType.OTHERS)
                    .setDatasetNameDelimiter(".")));
        break;
      case INSTANCE_URN:
        aspects.put(
            Constants.DATA_PLATFORM_INSTANCE_KEY_ASPECT_NAME,
            aspect(
                new DataPlatformInstanceKey()
                    .setPlatform(UrnUtils.getUrn(PLATFORM_URN))
                    .setInstance("prod")));
        break;
      default:
        throw new IllegalStateException("Unexpected urn " + urn);
    }
    return new EntityResponse().setUrn(urn).setEntityName(urn.getEntityType()).setAspects(aspects);
  }

  private static EnvelopedAspect aspect(RecordTemplate record) {
    return new EnvelopedAspect().setValue(new Aspect(record.data()));
  }
}
