package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MoveDataProductInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MoveDataProductResolver;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.service.DataProductService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MoveDataProductResolverTest {

  private static final String DOMAIN_URN = "urn:li:domain:test-domain";
  private static final String PARENT_URN = "urn:li:dataProduct:parent";
  private static final String CHILD_URN = "urn:li:dataProduct:child";
  private static final String GRANDCHILD_URN = "urn:li:dataProduct:grandchild";
  private static final CorpuserUrn TEST_ACTOR_URN = new CorpuserUrn("test");

  private static Domains domainsWith(String domainUrn) {
    return new Domains().setDomains(new UrnArray(UrnUtils.getUrn(domainUrn)));
  }

  private static EntityResponse propsResponse(Urn urn, Urn parentUrn) {
    DataProductProperties props = new DataProductProperties().setName(urn.getId());
    if (parentUrn != null) {
      props.setParentDataProduct(parentUrn);
    }
    Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(
        DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(props.data())));
    return new EntityResponse()
        .setEntityName(DATA_PRODUCT_ENTITY_NAME)
        .setUrn(urn)
        .setAspects(new EnvelopedAspectMap(aspects));
  }

  private void stubResolveParent(EntityClient client, Urn urn, Urn parentUrn) throws Exception {
    when(client.batchGetV2(
            any(),
            eq(DATA_PRODUCT_ENTITY_NAME),
            eq(Collections.singleton(urn)),
            eq(Collections.singleton(DATA_PRODUCT_PROPERTIES_ASPECT_NAME)),
            eq(false)))
        .thenReturn(Collections.singletonMap(urn, propsResponse(urn, parentUrn)));
  }

  private void setupAuthorizedMove(
      DataFetchingEnvironment mockEnv,
      EntityService<?> mockService,
      DataProductService mockDataProductService,
      String resourceUrn)
      throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(mockContext.getAuthentication()).thenReturn(mock(Authentication.class));
    when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    when(mockEnv.getContext()).thenReturn(mockContext);

    when(mockService.getAspect(
            any(),
            eq(Urn.createFromString(resourceUrn)),
            eq(DATA_PRODUCT_PROPERTIES_ASPECT_NAME),
            eq(0L)))
        .thenReturn(new DataProductProperties().setName("test"));

    when(mockDataProductService.getDataProductDomains(any(), eq(UrnUtils.getUrn(resourceUrn))))
        .thenReturn(domainsWith(DOMAIN_URN));
  }

  @Test
  public void testGetSuccess() throws Exception {
    EntityService<?> mockService = mock(EntityService.class);
    EntityClient mockClient = mock(EntityClient.class);
    DataProductService mockDataProductService = mock(DataProductService.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);

    MoveDataProductInput input = new MoveDataProductInput(PARENT_URN, CHILD_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockService.exists(any(OperationContext.class), eq(UrnUtils.getUrn(PARENT_URN)), eq(true)))
        .thenReturn(true);
    stubResolveParent(mockClient, UrnUtils.getUrn(PARENT_URN), null);
    setupAuthorizedMove(mockEnv, mockService, mockDataProductService, CHILD_URN);

    MoveDataProductResolver resolver =
        new MoveDataProductResolver(mockService, mockClient, mockDataProductService);
    assertTrue(resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(
            any(),
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
  }

  @Test
  public void testRejectsSelfParent() throws Exception {
    EntityService<?> mockService = mock(EntityService.class);
    EntityClient mockClient = mock(EntityClient.class);
    DataProductService mockDataProductService = mock(DataProductService.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);

    MoveDataProductInput input = new MoveDataProductInput(CHILD_URN, CHILD_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockService.exists(any(OperationContext.class), eq(UrnUtils.getUrn(CHILD_URN)), eq(true)))
        .thenReturn(true);
    setupAuthorizedMove(mockEnv, mockService, mockDataProductService, CHILD_URN);

    MoveDataProductResolver resolver =
        new MoveDataProductResolver(mockService, mockClient, mockDataProductService);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testRejectsMoveUnderDescendant() throws Exception {
    EntityService<?> mockService = mock(EntityService.class);
    EntityClient mockClient = mock(EntityClient.class);
    DataProductService mockDataProductService = mock(DataProductService.class);
    DataFetchingEnvironment mockEnv = mock(DataFetchingEnvironment.class);

    // PARENT -> CHILD -> GRANDCHILD. Moving PARENT under GRANDCHILD must fail.
    MoveDataProductInput input = new MoveDataProductInput(GRANDCHILD_URN, PARENT_URN);
    when(mockEnv.getArgument("input")).thenReturn(input);
    when(mockService.exists(
            any(OperationContext.class), eq(UrnUtils.getUrn(GRANDCHILD_URN)), eq(true)))
        .thenReturn(true);

    stubResolveParent(mockClient, UrnUtils.getUrn(GRANDCHILD_URN), UrnUtils.getUrn(CHILD_URN));
    stubResolveParent(mockClient, UrnUtils.getUrn(CHILD_URN), UrnUtils.getUrn(PARENT_URN));
    stubResolveParent(mockClient, UrnUtils.getUrn(PARENT_URN), null);

    setupAuthorizedMove(mockEnv, mockService, mockDataProductService, PARENT_URN);

    MoveDataProductResolver resolver =
        new MoveDataProductResolver(mockService, mockClient, mockDataProductService);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }
}
