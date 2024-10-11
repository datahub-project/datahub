package io.datahubproject.openapi.metadatatests;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.test.TestEngine;
import com.linkedin.metadata.test.definition.ValidationResult;
import com.linkedin.metadata.test.query.QueryEngine;
import com.linkedin.metadata.test.query.TestQuery;
import com.linkedin.metadata.test.query.TestQueryResponse;
import com.linkedin.test.TestDefinition;
import com.linkedin.test.TestDefinitionType;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestMode;
import com.linkedin.test.TestStatus;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.metadatatests.config.MetadataTestsTestConfiguration;
import io.datahubproject.openapi.metadatatests.generated.controller.MetadataTestApiController;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(
    basePackages = {"io.datahubproject.openapi", "com.linkedin.gms.factory.scim"},
    excludeFilters =
        @ComponentScan.Filter(
            type = FilterType.REGEX,
            pattern = "io\\.datahubproject\\.openapi\\.scim\\..*"))
@Import({MetadataTestsTestConfiguration.class})
@AutoConfigureMockMvc
public class MetadataTestsDelegateImplTest extends AbstractTestNGSpringContextTests {

  private static final Urn TEST_URN;
  private static final List<Urn> TEST_ENTITIES;

  static {
    try {
      TEST_URN = Urn.createFromString("urn:li:test:test123");

      TEST_ENTITIES =
          List.of(
              Urn.createFromString("urn:li:chart:(looker,baz1)"),
              Urn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"),
              Urn.createFromString(
                  "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"));
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Autowired private MetadataTestApiController metadataTestApiController;
  @Autowired private MockMvc mockMvc;
  @Autowired private EntityService<?> mockEntityService;
  @Autowired private QueryEngine mockQueryEngine;
  @Autowired private TestEngine mockTestEngine;

  @Test
  public void initTest() {
    assertNotNull(metadataTestApiController);
  }

  @Test
  public void executeTest() throws Exception {
    String testBody =
        "[\n"
            + String.format("  \"%s\",\n", TEST_ENTITIES.get(0))
            + String.format("  \"%s\",\n", TEST_ENTITIES.get(1))
            + String.format("  \"%s\"\n", TEST_ENTITIES.get(2))
            + "]";

    setupMockQueryEngine();
    setupMockTestInfo();

    mockMvc
        .perform(
            MockMvcRequestBuilders.post(
                    String.format("/v2/metadata_test/test/%s/evaluate", TEST_URN))
                .content(testBody)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .session(new MockHttpSession()))
        .andExpect(status().is2xxSuccessful())
        .andExpect(MockMvcResultMatchers.jsonPath("$.test").value(TEST_URN.toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$.testName").value("HasTags"))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.entities[0].entity")
                .value(TEST_ENTITIES.get(0).toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$.entities[0].type").doesNotExist())
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.entities[1].entity")
                .value(TEST_ENTITIES.get(1).toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$.entities[1].type").value("SUCCESS"))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.entities[2].entity")
                .value(TEST_ENTITIES.get(2).toString()))
        .andExpect(MockMvcResultMatchers.jsonPath("$.entities[2].type").value("FAILURE"));
  }

  private void setupMockQueryEngine() {
    when(mockQueryEngine.batchEvaluateQueries(any(OperationContext.class), any(), any()))
        .thenAnswer(
            args -> {
              Collection<TestQuery> testQueries = args.getArgument(2);
              TestQuery testQuery = testQueries.stream().findFirst().get();

              return Map.of(
                  TEST_ENTITIES.get(1), Map.of(testQuery, new TestQueryResponse(List.of("true"))),
                  TEST_ENTITIES.get(2), Map.of(testQuery, TestQueryResponse.empty()));
            });
    when(mockQueryEngine.validateQuery(any(TestQuery.class), anyList()))
        .thenReturn(ValidationResult.validResult());
  }

  private void setupMockTestInfo() throws URISyntaxException {
    // CHECKSTYLE:OFF
    final com.linkedin.entity.Aspect testInfoAspect =
        new com.linkedin.entity.Aspect(
            new TestInfo()
                .setName("HasTags")
                .setDefinition(
                    new TestDefinition()
                        .setType(TestDefinitionType.JSON)
                        .setJson(
                            "{\"on\":{\"types\":[\"dataset\"]},\"rules\":{\"and\":[{\"property\":\"globalTags.tags.tag\",\"operator\":\"exists\"}]}}"))
                .setStatus(new TestStatus().setMode(TestMode.ACTIVE))
                .data());
    // CHECKSTYLE:ON

    final EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setName(Constants.TEST_INFO_ASPECT_NAME);
    envelopedAspect.setVersion(ASPECT_LATEST_VERSION);
    envelopedAspect.setValue(testInfoAspect);
    envelopedAspect.setType(AspectType.VERSIONED);
    envelopedAspect.setCreated(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis()));

    EnvelopedAspectMap envelopedAspectMap = new EnvelopedAspectMap();
    envelopedAspectMap.putAll(Map.of(Constants.TEST_INFO_ASPECT_NAME, envelopedAspect));
    EntityResponse entityResponse =
        new EntityResponse()
            .setUrn(TEST_URN)
            .setEntityName(TEST_URN.getEntityType())
            .setAspects(envelopedAspectMap);
    entityResponse.setUrn(TEST_URN);

    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class),
            eq(Constants.TEST_ENTITY_NAME),
            eq(Set.of(TEST_URN)),
            eq(Set.of(Constants.TEST_INFO_ASPECT_NAME, Constants.STATUS_ASPECT_NAME))))
        .thenReturn(Map.of(TEST_URN, entityResponse));
  }
}
