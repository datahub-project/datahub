package io.datahubproject.openapi.v1.relationships;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizerChain;
import com.google.common.net.HttpHeaders;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.exception.UnauthorizedException;
import jakarta.servlet.http.HttpServletRequest;
import java.util.*;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.testng.annotations.*;

public class RelationshipsControllerTest {

  @Mock private GraphService graphService;

  @Mock private AuthorizerChain authorizerChain;

  @Mock private HttpServletRequest httpServletRequest;

  @Mock private Authentication authentication;

  @Mock private OperationContext mockOperationContext;

  @InjectMocks private RelationshipsController relationshipsController;

  private AutoCloseable mocks;
  private MockedStatic<AuthenticationContext> authenticationContextMock;
  private MockedStatic<AuthUtil> authUtilMock;
  private MockedStatic<OperationContext> operationContextMock;

  @BeforeMethod
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);

    // Mock static methods
    authenticationContextMock = mockStatic(AuthenticationContext.class);
    authUtilMock = mockStatic(AuthUtil.class);
    operationContextMock = mockStatic(OperationContext.class);

    // Setup authentication
    Actor actor = new Actor(ActorType.USER, "urn:li:corpuser:testuser");
    when(authentication.getActor()).thenReturn(actor);
    authenticationContextMock
        .when(AuthenticationContext::getAuthentication)
        .thenReturn(authentication);

    // Setup operation context
    operationContextMock
        .when(
            () ->
                OperationContext.asSession(
                    any(OperationContext.class),
                    any(),
                    any(AuthorizerChain.class),
                    any(Authentication.class),
                    anyBoolean()))
        .thenReturn(mockOperationContext);

    // Setup default metric utils behavior
    when(mockOperationContext.getMetricUtils()).thenReturn(java.util.Optional.empty());

    when(httpServletRequest.getHeader(eq(HttpHeaders.X_FORWARDED_FOR))).thenReturn("0.0.0.0");
  }

  @AfterMethod
  public void tearDown() throws Exception {
    authenticationContextMock.close();
    authUtilMock.close();
    operationContextMock.close();
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  public void testInitBinder() {
    WebDataBinder binder = mock(WebDataBinder.class);
    relationshipsController.initBinder(binder);
    verify(binder).registerCustomEditor(eq(String[].class), any());
  }

  @Test
  public void testGetRelationshipsWithEncodedUrn() {
    // Arrange
    String encodedUrn =
        "urn%3Ali%3Adataset%3A%28urn%3Ali%3AdataPlatform%3Ahive%2CSampleHiveDataset%2CPROD%29";
    String decodedUrn = "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)";
    String[] relationshipTypes = {"DownstreamOf"};
    RelationshipsController.RelationshipDirection direction =
        RelationshipsController.RelationshipDirection.OUTGOING;

    RelatedEntitiesResult expectedResult = new RelatedEntitiesResult(0, 10, 0, new ArrayList<>());

    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedUrns(any(OperationContext.class), any(), any(), any()))
        .thenReturn(true);

    when(graphService.findRelatedEntities(
            any(OperationContext.class),
            isNull(),
            argThat(filter -> filter.getCriteria().get(0).getValue().toString().equals(decodedUrn)),
            isNull(),
            any(Filter.class),
            any(Set.class),
            any(RelationshipFilter.class),
            anyInt(),
            anyInt()))
        .thenReturn(expectedResult);

    // Act
    ResponseEntity<RelatedEntitiesResult> response =
        relationshipsController.getRelationships(
            httpServletRequest, encodedUrn, relationshipTypes, direction, 0, 10);

    // Assert
    assertEquals(response.getStatusCode(), HttpStatus.OK);
  }

  @Test(expectedExceptions = UnauthorizedException.class)
  public void testGetRelationshipsUnauthorized() {
    // Arrange
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)";
    String[] relationshipTypes = {"DownstreamOf"};
    RelationshipsController.RelationshipDirection direction =
        RelationshipsController.RelationshipDirection.OUTGOING;

    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedUrns(any(OperationContext.class), any(), any(), any()))
        .thenReturn(false);

    // Act
    relationshipsController.getRelationships(
        httpServletRequest, urn, relationshipTypes, direction, 0, 10);
  }

  @Test
  public void testGetRelationshipsEmptyRelationshipTypes() {
    // Arrange
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)";
    String[] relationshipTypes = {};
    RelationshipsController.RelationshipDirection direction =
        RelationshipsController.RelationshipDirection.OUTGOING;

    RelatedEntitiesResult expectedResult = new RelatedEntitiesResult(0, 10, 0, new ArrayList<>());

    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedUrns(any(OperationContext.class), any(), any(), any()))
        .thenReturn(true);

    when(graphService.findRelatedEntities(
            any(OperationContext.class),
            isNull(),
            any(Filter.class),
            isNull(),
            any(Filter.class),
            argThat(Set::isEmpty),
            any(RelationshipFilter.class),
            anyInt(),
            anyInt()))
        .thenReturn(expectedResult);

    // Act
    ResponseEntity<RelatedEntitiesResult> response =
        relationshipsController.getRelationships(
            httpServletRequest, urn, relationshipTypes, direction, 0, 10);

    // Assert
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    assertEquals(response.getBody().getTotal(), 0);
  }

  @Test
  public void testGetRelationshipsWithMetrics() {
    // Arrange
    String urn = "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)";
    String[] relationshipTypes = {"DownstreamOf"};
    RelationshipsController.RelationshipDirection direction =
        RelationshipsController.RelationshipDirection.OUTGOING;

    RelatedEntitiesResult expectedResult = new RelatedEntitiesResult(0, 10, 1, new ArrayList<>());

    authUtilMock
        .when(() -> AuthUtil.isAPIAuthorizedUrns(any(OperationContext.class), any(), any(), any()))
        .thenReturn(true);

    when(graphService.findRelatedEntities(
            any(OperationContext.class),
            isNull(),
            any(Filter.class),
            isNull(),
            any(Filter.class),
            any(Set.class),
            any(RelationshipFilter.class),
            anyInt(),
            anyInt()))
        .thenReturn(expectedResult);

    // Mock metric utils
    when(mockOperationContext.getMetricUtils()).thenReturn(java.util.Optional.empty());

    // Act
    ResponseEntity<RelatedEntitiesResult> response =
        relationshipsController.getRelationships(
            httpServletRequest, urn, relationshipTypes, direction, 0, 10);

    // Assert
    assertEquals(response.getStatusCode(), HttpStatus.OK);
    verify(mockOperationContext, atLeastOnce()).getMetricUtils();
  }
}
