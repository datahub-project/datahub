package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.MoveDomainInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MoveDomainResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.DomainUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class MoveDomainResolverTest {

  private static final String CONTAINER_URN = "urn:li:container:00005397daf94708a8822b8106cfd451";
  private static final String PARENT_DOMAIN_URN = "urn:li:domain:00005397daf94708a8822b8106cfd451";
  private static final String DOMAIN_URN = "urn:li:domain:11115397daf94708a8822b8106cfd451";
  private static final MoveDomainInput INPUT = new MoveDomainInput(PARENT_DOMAIN_URN, DOMAIN_URN);
  private static final MoveDomainInput INVALID_INPUT =
      new MoveDomainInput(CONTAINER_URN, DOMAIN_URN);
  private static final CorpuserUrn TEST_ACTOR_URN = new CorpuserUrn("test");

  private MetadataChangeProposal setupTests(
      DataFetchingEnvironment mockEnv, EntityService mockService, EntityClient mockClient)
      throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final String name = "test name";
    Mockito.when(
            mockService.getAspect(
                Urn.createFromString(DOMAIN_URN), Constants.DOMAIN_PROPERTIES_ASPECT_NAME, 0))
        .thenReturn(new DomainProperties().setName(name));

    Mockito.when(
            mockClient.filter(
                Mockito.eq(Constants.DOMAIN_ENTITY_NAME),
                Mockito.eq(
                    DomainUtils.buildNameAndParentDomainFilter(
                        name, Urn.createFromString(PARENT_DOMAIN_URN))),
                Mockito.eq(null),
                Mockito.any(Integer.class),
                Mockito.any(Integer.class),
                Mockito.any(Authentication.class)))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));

    DomainProperties properties = new DomainProperties();
    properties.setName(name);
    properties.setParentDomain(Urn.createFromString(PARENT_DOMAIN_URN));
    return MutationUtils.buildMetadataChangeProposalWithUrn(
        Urn.createFromString(DOMAIN_URN), DOMAIN_PROPERTIES_ASPECT_NAME, properties);
  }

  @Test
  public void testGetSuccess() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(Urn.createFromString(PARENT_DOMAIN_URN))).thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    MoveDomainResolver resolver = new MoveDomainResolver(mockService, mockClient);
    setupTests(mockEnv, mockService, mockClient);

    assertTrue(resolver.get(mockEnv).get());
    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
  }

  @Test
  public void testGetFailureEntityDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(Urn.createFromString(PARENT_DOMAIN_URN))).thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    Mockito.when(
            mockService.getAspect(
                Urn.createFromString(DOMAIN_URN), DOMAIN_PROPERTIES_ASPECT_NAME, 0))
        .thenReturn(null);

    MoveDomainResolver resolver = new MoveDomainResolver(mockService, mockClient);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureParentDoesNotExist() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(Urn.createFromString(PARENT_DOMAIN_URN))).thenReturn(false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INPUT);

    MoveDomainResolver resolver = new MoveDomainResolver(mockService, mockClient);
    setupTests(mockEnv, mockService, mockClient);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailureParentIsNotDomain() throws Exception {
    EntityService mockService = Mockito.mock(EntityService.class);
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.when(mockService.exists(Urn.createFromString(PARENT_DOMAIN_URN))).thenReturn(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(INVALID_INPUT);

    MoveDomainResolver resolver = new MoveDomainResolver(mockService, mockClient);
    setupTests(mockEnv, mockService, mockClient);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }
}
