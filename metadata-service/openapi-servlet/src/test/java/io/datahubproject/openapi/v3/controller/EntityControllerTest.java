package io.datahubproject.openapi.v3.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertNotNull;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.SearchUtil;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureWebMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@ComponentScan(basePackages = {"io.datahubproject.openapi.v3.controller"})
@Import({SpringWebConfig.class, EntityControllerTest.EntityControllerTestConfig.class})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class EntityControllerTest extends AbstractTestNGSpringContextTests {
  @Autowired private EntityController entityController;
  @Autowired private MockMvc mockMvc;
  @Autowired private SearchService mockSearchService;
  @Autowired private EntityService<?> mockEntityService;

  @Test
  public void initTest() {
    assertNotNull(entityController);
  }

  @Test
  public void testSearchOrderPreserved() throws Exception {
    List<Urn> TEST_URNS =
        List.of(
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:testPlatform,1,PROD)"),
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:testPlatform,2,PROD)"),
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:testPlatform,3,PROD)"));

    // Mock scroll ascending/descending results
    ScrollResult expectedResultAscending =
        new ScrollResult()
            .setEntities(
                new SearchEntityArray(
                    List.of(
                        new SearchEntity().setEntity(TEST_URNS.get(0)),
                        new SearchEntity().setEntity(TEST_URNS.get(1)),
                        new SearchEntity().setEntity(TEST_URNS.get(2)))));
    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataset")),
            anyString(),
            nullable(Filter.class),
            eq(Collections.singletonList(SearchUtil.sortBy("urn", SortOrder.valueOf("ASCENDING")))),
            nullable(String.class),
            nullable(String.class),
            anyInt()))
        .thenReturn(expectedResultAscending);
    ScrollResult expectedResultDescending =
        new ScrollResult()
            .setEntities(
                new SearchEntityArray(
                    List.of(
                        new SearchEntity().setEntity(TEST_URNS.get(2)),
                        new SearchEntity().setEntity(TEST_URNS.get(1)),
                        new SearchEntity().setEntity(TEST_URNS.get(0)))));
    when(mockSearchService.scrollAcrossEntities(
            any(OperationContext.class),
            eq(List.of("dataset")),
            anyString(),
            nullable(Filter.class),
            eq(
                Collections.singletonList(
                    SearchUtil.sortBy("urn", SortOrder.valueOf("DESCENDING")))),
            nullable(String.class),
            nullable(String.class),
            anyInt()))
        .thenReturn(expectedResultDescending);
    // Mock entity aspect
    when(mockEntityService.getEnvelopedVersionedAspects(
            any(OperationContext.class), anyMap(), eq(false)))
        .thenReturn(
            Map.of(
                TEST_URNS.get(0),
                    List.of(
                        new EnvelopedAspect()
                            .setName("status")
                            .setValue(new Aspect(new Status().data()))),
                TEST_URNS.get(1),
                    List.of(
                        new EnvelopedAspect()
                            .setName("status")
                            .setValue(new Aspect(new Status().data()))),
                TEST_URNS.get(2),
                    List.of(
                        new EnvelopedAspect()
                            .setName("status")
                            .setValue(new Aspect(new Status().data())))));

    // test ASCENDING
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/v3/entity/dataset")
                .param("sortOrder", "ASCENDING")
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.entities[0].urn").value(TEST_URNS.get(0).toString()))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.entities[1].urn").value(TEST_URNS.get(1).toString()))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.entities[2].urn").value(TEST_URNS.get(2).toString()));

    // test DESCENDING
    mockMvc
        .perform(
            MockMvcRequestBuilders.get("/v3/entity/dataset")
                .accept(MediaType.APPLICATION_JSON)
                .param("sortOrder", "DESCENDING"))
        .andExpect(status().is2xxSuccessful())
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.entities[0].urn").value(TEST_URNS.get(2).toString()))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.entities[1].urn").value(TEST_URNS.get(1).toString()))
        .andExpect(
            MockMvcResultMatchers.jsonPath("$.entities[2].urn").value(TEST_URNS.get(0).toString()));
  }

  @TestConfiguration
  public static class EntityControllerTestConfig {
    @MockBean public EntityServiceImpl entityService;
    @MockBean public SearchService searchService;

    @Bean
    public ObjectMapper objectMapper() {
      return new ObjectMapper();
    }

    @Bean(name = "systemOperationContext")
    public OperationContext systemOperationContext() {
      return TestOperationContexts.systemContextNoSearchAuthorization();
    }

    @Bean("entityRegistry")
    @Primary
    public EntityRegistry entityRegistry(
        @Qualifier("systemOperationContext") final OperationContext testOperationContext) {
      return testOperationContext.getEntityRegistry();
    }

    @Bean("graphService")
    @Primary
    public ElasticSearchGraphService graphService() {
      return mock(ElasticSearchGraphService.class);
    }

    @Bean
    public AuthorizerChain authorizerChain() {
      AuthorizerChain authorizerChain = mock(AuthorizerChain.class);

      Authentication authentication = mock(Authentication.class);
      when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
      when(authorizerChain.authorize(any()))
          .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
      AuthenticationContext.setAuthentication(authentication);

      return authorizerChain;
    }
  }
}
