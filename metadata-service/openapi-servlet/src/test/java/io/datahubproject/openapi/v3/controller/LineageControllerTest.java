package io.datahubproject.openapi.v3.controller;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.authorization.ApiGroup;
import com.linkedin.metadata.authorization.ApiOperation;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.LineageDirection;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.openapi.config.GlobalControllerExceptionHandler;
import io.datahubproject.openapi.config.SpringWebConfig;
import io.datahubproject.openapi.config.TracingInterceptor;
import io.datahubproject.openapi.v3.models.ScrollLineageRequestBody;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureWebMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest(classes = {SpringWebConfig.class})
@Import({
  SpringWebConfig.class,
  TracingInterceptor.class,
  LineageController.class,
  LineageControllerTest.LineageControllerTestConfig.class,
  GlobalControllerExceptionHandler.class,
})
@AutoConfigureWebMvc
@AutoConfigureMockMvc
public class LineageControllerTest extends AbstractTestNGSpringContextTests {
  @MockitoBean private ConfigurationProvider configurationProvider;
  @MockitoBean private EntityRegistry entityRegistry;
  @MockitoBean private SystemTelemetryContext systemTelemetryContext;

  @Autowired private LineageController lineageController;
  @Autowired private MockMvc mockMvc;
  @Autowired private GraphService mockGraphService;
  @Autowired private ObjectMapper objectMapper;

  private static final String ANCHOR_URN = "urn:li:dataset:(urn:li:dataPlatform:hive,anchor,PROD)";
  private static final String UPSTREAM_PEER_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,upstream_peer,PROD)";
  private static final String DOWNSTREAM_PEER_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,downstream_peer,PROD)";

  private static final ScrollLineageRequestBody EMPTY_SCROLL_BODY =
      ScrollLineageRequestBody.builder().build();

  @BeforeMethod
  public void setup() {
    org.mockito.MockitoAnnotations.openMocks(this);
    // mockGraphService is a Spring-singleton bean shared across all test methods in this class —
    // reset it so stubs and invocation history don't leak between tests (needed for the never()
    // verification in testScrollLineageUnauthorizedOnEntryReturnsForbidden).
    reset(mockGraphService);
  }

  @TestConfiguration
  public static class LineageControllerTestConfig {

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

  @Test
  public void initTest() {
    assertNotNull(lineageController);
  }

  /**
   * Stubs {@code mockGraphService.getLineageRegistry()} with a mock registry that has no lineage
   * triplets (empty {@code getLineageSpecs()}) but classifies {@code relationshipType} as {@code
   * upstreamOfDirection} for {@code UPSTREAM} and {@code downstreamOfDirection} for {@code
   * DOWNSTREAM}, for the given {@code entityType}.
   */
  private LineageRegistry stubLineageRegistry(
      String entityType,
      String relationshipType,
      RelationshipDirection upstreamOfDirection,
      RelationshipDirection downstreamOfDirection) {
    LineageRegistry lineageRegistry = mock(LineageRegistry.class);
    when(lineageRegistry.getLineageSpecs()).thenReturn(Collections.emptyMap());
    when(lineageRegistry.getLineageRelationships(entityType, LineageDirection.UPSTREAM))
        .thenReturn(
            List.of(
                new LineageRegistry.EdgeInfo(relationshipType, upstreamOfDirection, entityType)));
    when(lineageRegistry.getLineageRelationships(entityType, LineageDirection.DOWNSTREAM))
        .thenReturn(
            List.of(
                new LineageRegistry.EdgeInfo(relationshipType, downstreamOfDirection, entityType)));
    when(mockGraphService.getLineageRegistry()).thenReturn(lineageRegistry);
    return lineageRegistry;
  }

  private void stubScrollRelatedEntities(RelatedEntitiesScrollResult result) {
    when(mockGraphService.scrollRelatedEntities(
            any(), any(), any(), isNull(), anyString(), anyInt(), isNull(), isNull()))
        .thenReturn(result);
  }

  @Test
  public void testScrollLineageSetsAllowedEdgeTriplets() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, "lineage-scroll", Arrays.asList());

    LineageRegistry lineageRegistry =
        TestOperationContexts.systemContextNoSearchAuthorization().getLineageRegistry();
    when(mockGraphService.getLineageRegistry()).thenReturn(lineageRegistry);

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);
    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.scrollId").value("lineage-scroll"));

    GraphFilters captured = graphFiltersCaptor.getValue();
    // scrollLineage should populate allowedEdgeTriplets from the lineage registry
    assertNotNull(captured.getAllowedEdgeTriplets());
    assertFalse(captured.getAllowedEdgeTriplets().isEmpty());

    // Verify a known lineage edge is present (dataset DownstreamOf dataset)
    boolean hasDownstreamOf =
        captured.getAllowedEdgeTriplets().stream()
            .anyMatch(
                p -> p.getKey().equals("dataset") && p.getValue().getType().equals("DownstreamOf"));
    assertTrue(hasDownstreamOf, "Expected lineage triplets to include dataset DownstreamOf");
  }

  @Test
  public void testScrollLineageWithUrnsAndRelationshipTypes() throws Exception {
    RelatedEntitiesScrollResult expectedResult =
        new RelatedEntitiesScrollResult(0, 10, null, Arrays.asList());

    LineageRegistry lineageRegistry =
        TestOperationContexts.systemContextNoSearchAuthorization().getLineageRegistry();
    when(mockGraphService.getLineageRegistry()).thenReturn(lineageRegistry);

    ArgumentCaptor<GraphFilters> graphFiltersCaptor = ArgumentCaptor.forClass(GraphFilters.class);
    when(mockGraphService.scrollRelatedEntities(
            any(),
            graphFiltersCaptor.capture(),
            any(),
            isNull(),
            anyString(),
            anyInt(),
            isNull(),
            isNull()))
        .thenReturn(expectedResult);

    ScrollLineageRequestBody body =
        ScrollLineageRequestBody.builder()
            .urns(List.of("urn:li:dataset:(urn:li:dataPlatform:hive,foo,PROD)"))
            .build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                .param("relationshipTypes", "Consumes")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful());

    GraphFilters captured = graphFiltersCaptor.getValue();
    // sourceTypes/destinationTypes are no longer supported on this endpoint — the graph is
    // always queried undirected, and the urns are matched via the general edge filter.
    assertNull(captured.getSourceTypes());
    assertNull(captured.getDestinationTypes());
    assertEquals(Set.of("Consumes"), captured.getRelationshipTypes());
    assertEquals(RelationshipDirection.UNDIRECTED, captured.getRelationshipDirection());
    assertFalse(captured.getRelationshipFilter().getOr().isEmpty());
    assertNotNull(captured.getAllowedEdgeTriplets());
    assertFalse(captured.getAllowedEdgeTriplets().isEmpty());
  }

  @Test
  public void testScrollLineageWithInvalidDirection() throws Exception {
    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                .param("direction", "SIDEWAYS")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is4xxClientError());
  }

  /**
   * A mixed pair of edges relative to {@link #ANCHOR_URN}: one that is UPSTREAM of the anchor
   * (anchor consumes from upstream_peer) and one that is DOWNSTREAM (downstream_peer consumes from
   * anchor).
   */
  private List<RelatedEntities> mixedDirectionEdges() {
    RelatedEntities upstreamEdge =
        new RelatedEntities(
            "Consumes", ANCHOR_URN, UPSTREAM_PEER_URN, RelationshipDirection.OUTGOING, null);
    RelatedEntities downstreamEdge =
        new RelatedEntities(
            "Consumes", DOWNSTREAM_PEER_URN, ANCHOR_URN, RelationshipDirection.OUTGOING, null);
    return Arrays.asList(upstreamEdge, downstreamEdge);
  }

  @Test
  public void testScrollLineageNoDirectionReturnsBothEndpointMatches() throws Exception {
    // anchor -Consumes-> upstream_peer classifies as UPSTREAM-of-anchor;
    // downstream_peer -Consumes-> anchor classifies as DOWNSTREAM-of-anchor.
    stubLineageRegistry(
        "dataset", "Consumes", RelationshipDirection.OUTGOING, RelationshipDirection.INCOMING);
    stubScrollRelatedEntities(
        new RelatedEntitiesScrollResult(2, 10, "scroll-mixed", mixedDirectionEdges()));

    ScrollLineageRequestBody body =
        ScrollLineageRequestBody.builder().urns(List.of(ANCHOR_URN)).build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.results.length()").value(2))
        .andExpect(jsonPath("$.scrollId").value("scroll-mixed"));
  }

  @Test
  public void testScrollLineageDirectionWithoutUrnsIsNoOp() throws Exception {
    // direction is set but urns is absent — per the documented contract, direction has no effect
    // without an anchor, so both edges should still be returned unfiltered.
    stubLineageRegistry(
        "dataset", "Consumes", RelationshipDirection.OUTGOING, RelationshipDirection.INCOMING);
    stubScrollRelatedEntities(
        new RelatedEntitiesScrollResult(2, 10, "scroll-mixed", mixedDirectionEdges()));

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                .param("direction", "UPSTREAM")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.results.length()").value(2));
  }

  @Test
  public void testScrollLineageUpstreamDirectionFiltersResults() throws Exception {
    stubLineageRegistry(
        "dataset", "Consumes", RelationshipDirection.OUTGOING, RelationshipDirection.INCOMING);
    stubScrollRelatedEntities(
        new RelatedEntitiesScrollResult(2, 10, "scroll-mixed", mixedDirectionEdges()));

    ScrollLineageRequestBody body =
        ScrollLineageRequestBody.builder().urns(List.of(ANCHOR_URN)).build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                .param("direction", "UPSTREAM")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.results.length()").value(1))
        .andExpect(jsonPath("$.results[0].upstream").value(UPSTREAM_PEER_URN))
        .andExpect(jsonPath("$.results[0].downstream").value(ANCHOR_URN))
        // scrollId must be preserved even though the post-filter dropped an edge from this page.
        .andExpect(jsonPath("$.scrollId").value("scroll-mixed"));
  }

  @Test
  public void testScrollLineageDownstreamDirectionFiltersResults() throws Exception {
    stubLineageRegistry(
        "dataset", "Consumes", RelationshipDirection.OUTGOING, RelationshipDirection.INCOMING);
    stubScrollRelatedEntities(
        new RelatedEntitiesScrollResult(2, 10, "scroll-mixed", mixedDirectionEdges()));

    ScrollLineageRequestBody body =
        ScrollLineageRequestBody.builder().urns(List.of(ANCHOR_URN)).build();

    mockMvc
        .perform(
            MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                .param("direction", "DOWNSTREAM")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(body))
                .accept(MediaType.APPLICATION_JSON))
        .andExpect(status().is2xxSuccessful())
        .andExpect(jsonPath("$.results.length()").value(1))
        .andExpect(jsonPath("$.results[0].upstream").value(ANCHOR_URN))
        .andExpect(jsonPath("$.results[0].downstream").value(DOWNSTREAM_PEER_URN))
        .andExpect(jsonPath("$.scrollId").value("scroll-mixed"));
  }

  @Test
  public void testScrollLineageUnauthorizedOnEntryReturnsForbidden() throws Exception {
    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class), any(ApiGroup.class), any(ApiOperation.class)))
          .thenReturn(false);

      mockMvc
          .perform(
              MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().isForbidden());

      verify(mockGraphService, never())
          .scrollRelatedEntities(any(), any(), any(), any(), any(), any(), any(), any());
    }
  }

  @Test
  public void testScrollLineageUnauthorizedOnResultsReturnsForbidden() throws Exception {
    stubLineageRegistry(
        "dataset", "Consumes", RelationshipDirection.OUTGOING, RelationshipDirection.INCOMING);
    stubScrollRelatedEntities(
        new RelatedEntitiesScrollResult(2, 10, "scroll-mixed", mixedDirectionEdges()));

    try (MockedStatic<AuthUtil> authUtilMock = mockStatic(AuthUtil.class)) {
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorized(
                      any(OperationContext.class), any(ApiGroup.class), any(ApiOperation.class)))
          .thenReturn(true);
      authUtilMock
          .when(
              () ->
                  AuthUtil.isAPIAuthorizedUrns(
                      any(OperationContext.class),
                      any(ApiGroup.class),
                      any(ApiOperation.class),
                      any()))
          .thenReturn(false);

      mockMvc
          .perform(
              MockMvcRequestBuilders.post("/openapi/v3/lineage/scroll")
                  .contentType(MediaType.APPLICATION_JSON)
                  .content(objectMapper.writeValueAsString(EMPTY_SCROLL_BODY))
                  .accept(MediaType.APPLICATION_JSON))
          .andExpect(status().isForbidden());
    }
  }
}
