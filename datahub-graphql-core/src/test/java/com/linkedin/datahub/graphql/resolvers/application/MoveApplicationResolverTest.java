package com.linkedin.datahub.graphql.resolvers.application;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.application.ApplicationProperties;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MoveApplicationInput;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MoveApplicationResolverTest {

  private static final Urn APP_URN = UrnUtils.getUrn("urn:li:application:child");
  private static final Urn PARENT_URN = UrnUtils.getUrn("urn:li:application:parent");

  private static DataFetchingEnvironment env(QueryContext context, String parent, Urn resource) {
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(context);
    Mockito.when(mockEnv.getArgument("input"))
        .thenReturn(new MoveApplicationInput(parent, resource.toString()));
    return mockEnv;
  }

  private static EntityService<?> serviceWithApplication() {
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockService.getAspect(
                any(), eq(APP_URN), eq(APPLICATION_PROPERTIES_ASPECT_NAME), eq(0L)))
        .thenReturn(new ApplicationProperties().setName("child"));
    Mockito.when(mockService.exists(any(), eq(PARENT_URN), eq(true))).thenReturn(true);
    return mockService;
  }

  private static ApplicationProperties ingestedProperties(EntityService<?> mockService) {
    ArgumentCaptor<MetadataChangeProposal> proposal =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(any(), proposal.capture(), any(AuditStamp.class), eq(false));
    return GenericRecordUtils.deserializeAspect(
        proposal.getValue().getAspect().getValue(),
        proposal.getValue().getAspect().getContentType(),
        ApplicationProperties.class);
  }

  @Test
  public void testSetsParent() throws Exception {
    EntityService<?> mockService = serviceWithApplication();
    MoveApplicationResolver resolver = new MoveApplicationResolver(mockService);

    assertTrue(resolver.get(env(getMockAllowContext(), PARENT_URN.toString(), APP_URN)).get());

    ApplicationProperties written = ingestedProperties(mockService);
    assertEquals(written.getParentApplication(), PARENT_URN);
    assertEquals(written.getName(), "child");
  }

  @Test
  public void testNullParentClearsParent() throws Exception {
    EntityService<?> mockService = Mockito.mock(EntityService.class);
    Mockito.when(
            mockService.getAspect(
                any(), eq(APP_URN), eq(APPLICATION_PROPERTIES_ASPECT_NAME), eq(0L)))
        .thenReturn(new ApplicationProperties().setName("child").setParentApplication(PARENT_URN));
    MoveApplicationResolver resolver = new MoveApplicationResolver(mockService);

    assertTrue(resolver.get(env(getMockAllowContext(), null, APP_URN)).get());

    assertFalse(ingestedProperties(mockService).hasParentApplication());
  }

  @Test
  public void testRejectsInvalidParents() throws Exception {
    EntityService<?> mockService = serviceWithApplication();
    Mockito.when(
            mockService.exists(any(), eq(UrnUtils.getUrn("urn:li:application:gone")), eq(true)))
        .thenReturn(false);
    MoveApplicationResolver resolver = new MoveApplicationResolver(mockService);

    for (String parent :
        new String[] {"urn:li:domain:not-an-app", "urn:li:application:gone", APP_URN.toString()}) {
      assertThrows(
          CompletionException.class,
          () -> resolver.get(env(getMockAllowContext(), parent, APP_URN)).join());
    }
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testRejectsMoveUnderOwnDescendant() throws Exception {
    // PARENT_URN currently sits under APP_URN, so making it APP_URN's parent would loop.
    EntityService<?> mockService = serviceWithApplication();
    QueryContext context = getMockAllowContext();
    OperationContext base = context.getOperationContext();
    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              java.util.Set<Urn> urns = invocation.getArgument(1);
              if (urns.contains(PARENT_URN)) {
                return Map.of(
                    PARENT_URN,
                    Map.of(
                        APPLICATION_PROPERTIES_ASPECT_NAME,
                        new Aspect(
                            new ApplicationProperties()
                                .setName("parent")
                                .setParentApplication(APP_URN)
                                .data())));
              }
              return Map.of();
            });
    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(base.getRetrieverContext().getGraphRetriever())
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(base.getRetrieverContext().getCachingAspectRetriever())
            .searchRetriever(base.getRetrieverContext().getSearchRetriever())
            .build();
    OperationContext operationContext =
        base.toBuilder()
            .retrieverContext(retrieverContext)
            .build(base.getSessionAuthentication(), false);
    Mockito.when(context.getOperationContext()).thenReturn(operationContext);
    MoveApplicationResolver resolver = new MoveApplicationResolver(mockService);

    assertThrows(
        CompletionException.class,
        () -> resolver.get(env(context, PARENT_URN.toString(), APP_URN)).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testUnauthorized() throws Exception {
    EntityService<?> mockService = serviceWithApplication();
    MoveApplicationResolver resolver = new MoveApplicationResolver(mockService);

    assertThrows(
        CompletionException.class,
        () -> resolver.get(env(getMockDenyContext(), PARENT_URN.toString(), APP_URN)).join());
    verifyNoIngestProposal(mockService);
  }
}
