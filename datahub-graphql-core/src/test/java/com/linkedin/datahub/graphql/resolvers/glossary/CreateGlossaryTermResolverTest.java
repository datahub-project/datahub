package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockEntityService;
import static com.linkedin.metadata.Constants.*;
import static org.testng.Assert.assertThrows;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateGlossaryEntityInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.GlossaryTermKey;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateGlossaryTermResolverTest {

  private static final String EXISTING_TERM_URN = "urn:li:glossaryTerm:testing12345";

  private static final CreateGlossaryEntityInput TEST_INPUT =
      new CreateGlossaryEntityInput(
          "test-id",
          "test-name",
          "test-description",
          "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b");
  private static final CreateGlossaryEntityInput TEST_INPUT_NO_DESCRIPTION =
      new CreateGlossaryEntityInput(
          "test-id", "test-name", null, "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b");

  private static final CreateGlossaryEntityInput TEST_INPUT_NO_PARENT_NODE =
      new CreateGlossaryEntityInput("test-id", "test-name", "test-description", null);

  private final String parentNodeUrn = "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b";

  private MetadataChangeProposal setupTest(
      DataFetchingEnvironment mockEnv,
      CreateGlossaryEntityInput input,
      String description,
      String parentNode)
      throws Exception {
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    final GlossaryTermKey key = new GlossaryTermKey();
    key.setName("test-id");
    GlossaryTermInfo props = new GlossaryTermInfo();
    props.setDefinition(description);
    props.setName("test-name");
    props.setTermSource("INTERNAL");
    if (parentNode != null) {
      final GlossaryNodeUrn parent = GlossaryNodeUrn.createFromString(parentNode);
      props.setParentNode(parent);
    }
    return MutationUtils.buildMetadataChangeProposalWithKey(
        key, GLOSSARY_TERM_ENTITY_NAME, GLOSSARY_TERM_INFO_ASPECT_NAME, props);
  }

  @Test
  public void testGetSuccess() throws Exception {
    EntityClient mockClient = initMockClient();
    EntityService mockService = getMockEntityService();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final MetadataChangeProposal proposal =
        setupTest(mockEnv, TEST_INPUT, "test-description", parentNodeUrn);

    CreateGlossaryTermResolver resolver = new CreateGlossaryTermResolver(mockClient, mockService);
    resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(Mockito.eq(proposal), Mockito.any(Authentication.class), Mockito.eq(false));
  }

  @Test
  public void testGetSuccessNoDescription() throws Exception {
    EntityClient mockClient = initMockClient();
    EntityService mockService = getMockEntityService();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final MetadataChangeProposal proposal =
        setupTest(mockEnv, TEST_INPUT_NO_DESCRIPTION, "", parentNodeUrn);

    CreateGlossaryTermResolver resolver = new CreateGlossaryTermResolver(mockClient, mockService);
    resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(Mockito.eq(proposal), Mockito.any(Authentication.class), Mockito.eq(false));
  }

  @Test
  public void testGetSuccessNoParentNode() throws Exception {
    EntityClient mockClient = initMockClient();
    EntityService mockService = getMockEntityService();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    final MetadataChangeProposal proposal =
        setupTest(mockEnv, TEST_INPUT_NO_PARENT_NODE, "test-description", null);

    CreateGlossaryTermResolver resolver = new CreateGlossaryTermResolver(mockClient, mockService);
    resolver.get(mockEnv).get();

    Mockito.verify(mockClient, Mockito.times(1))
        .ingestProposal(Mockito.eq(proposal), Mockito.any(Authentication.class), Mockito.eq(false));
  }

  @Test
  public void testGetFailureExistingTermSameName() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.filter(
                Mockito.eq(GLOSSARY_TERM_ENTITY_NAME),
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(0),
                Mockito.eq(1000),
                Mockito.any()))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(
                        new SearchEntity().setEntity(UrnUtils.getUrn(EXISTING_TERM_URN)))));

    Map<Urn, EntityResponse> result = new HashMap<>();
    EnvelopedAspectMap map = new EnvelopedAspectMap();
    GlossaryTermInfo termInfo = new GlossaryTermInfo().setName("Duplicated Name");
    map.put(
        GLOSSARY_TERM_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(termInfo.data())));
    result.put(UrnUtils.getUrn(EXISTING_TERM_URN), new EntityResponse().setAspects(map));

    Mockito.when(
            mockClient.batchGetV2(
                Mockito.eq(GLOSSARY_TERM_ENTITY_NAME),
                Mockito.any(),
                Mockito.eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME)),
                Mockito.any()))
        .thenReturn(result);

    EntityService mockService = getMockEntityService();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    CreateGlossaryEntityInput input =
        new CreateGlossaryEntityInput(
            "test-id",
            "Duplicated Name",
            "test-description",
            "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b");
    setupTest(mockEnv, input, "test-description", parentNodeUrn);
    CreateGlossaryTermResolver resolver = new CreateGlossaryTermResolver(mockClient, mockService);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockClient, Mockito.times(0))
        .ingestProposal(Mockito.any(), Mockito.any(Authentication.class));
  }

  private EntityClient initMockClient() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    Mockito.when(
            mockClient.filter(
                Mockito.eq(GLOSSARY_TERM_ENTITY_NAME),
                Mockito.any(),
                Mockito.eq(null),
                Mockito.eq(0),
                Mockito.eq(1000),
                Mockito.any()))
        .thenReturn(new SearchResult().setEntities(new SearchEntityArray()));
    Mockito.when(
            mockClient.batchGetV2(
                Mockito.eq(GLOSSARY_TERM_ENTITY_NAME),
                Mockito.any(),
                Mockito.eq(Collections.singleton(GLOSSARY_TERM_INFO_ASPECT_NAME)),
                Mockito.any()))
        .thenReturn(new HashMap<>());

    return mockClient;
  }
}
