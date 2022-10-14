package com.linkedin.datahub.graphql.resolvers.glossary;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.RelatedTermsInput;
import com.linkedin.datahub.graphql.generated.TermRelationshipType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.ExecutionException;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.testng.Assert.*;


public class AddRelatedTermsResolverTest {

  private static final String TEST_ENTITY_URN = "urn:li:glossaryTerm:test-id-0";
  private static final String TEST_TERM_1_URN = "urn:li:glossaryTerm:test-id-1";
  private static final String TEST_TERM_2_URN = "urn:li:glossaryTerm:test-id-2";
  private static final String DATASET_URN = "urn:li:dataset:(test,test,test)";

  private EntityService setUpService() {
    EntityService mockService = Mockito.mock(EntityService.class);
    Mockito.when(mockService.getAspect(
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
            Mockito.eq(Constants.GLOSSARY_RELATED_TERM_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(null);
    return mockService;
  }

  @Test
  public void testGetSuccessIsRelatedNonExistent() throws Exception {
    EntityService mockService = setUpService();

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_2_URN))).thenReturn(true);

    AddRelatedTermsResolver resolver = new AddRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        TEST_TERM_1_URN,
        TEST_TERM_2_URN
    ), TermRelationshipType.isA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    verifyIngestProposal(mockService, 1);
    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN))
    );
    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_TERM_1_URN))
    );
    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_TERM_2_URN))
    );
  }

  @Test
  public void testGetSuccessHasRelatedNonExistent() throws Exception {
    EntityService mockService = setUpService();

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_2_URN))).thenReturn(true);

    AddRelatedTermsResolver resolver = new AddRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        TEST_TERM_1_URN,
        TEST_TERM_2_URN
    ), TermRelationshipType.hasA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    verifyIngestProposal(mockService, 1);
    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_ENTITY_URN))
    );
    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_TERM_1_URN))
    );
    Mockito.verify(mockService, Mockito.times(1)).exists(
        Mockito.eq(Urn.createFromString(TEST_TERM_2_URN))
    );
  }

  @Test
  public void testGetFailAddSelfAsRelatedTerm() throws Exception {
    EntityService mockService = setUpService();

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    AddRelatedTermsResolver resolver = new AddRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        TEST_ENTITY_URN
    ), TermRelationshipType.hasA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailAddNonTermAsRelatedTerm() throws Exception {
    EntityService mockService = setUpService();

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);

    AddRelatedTermsResolver resolver = new AddRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        DATASET_URN
    ), TermRelationshipType.hasA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailAddNonExistentTermAsRelatedTerm() throws Exception {
    EntityService mockService = setUpService();

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_1_URN))).thenReturn(false);

    AddRelatedTermsResolver resolver = new AddRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        TEST_TERM_1_URN
    ), TermRelationshipType.hasA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailAddToNonExistentUrn() throws Exception {
    EntityService mockService = setUpService();

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(false);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_1_URN))).thenReturn(true);

    AddRelatedTermsResolver resolver = new AddRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        TEST_TERM_1_URN
    ), TermRelationshipType.hasA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetFailAddToNonTerm() throws Exception {
    EntityService mockService = setUpService();

    Mockito.when(mockService.exists(Urn.createFromString(DATASET_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_1_URN))).thenReturn(true);

    AddRelatedTermsResolver resolver = new AddRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(DATASET_URN, ImmutableList.of(
        TEST_TERM_1_URN
    ), TermRelationshipType.hasA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testFailNoPermissions() throws Exception {
    EntityService mockService = setUpService();

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_2_URN))).thenReturn(true);

    AddRelatedTermsResolver resolver = new AddRelatedTermsResolver(mockService);

    QueryContext mockContext = getMockDenyContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    RelatedTermsInput input = new RelatedTermsInput(TEST_ENTITY_URN, ImmutableList.of(
        TEST_TERM_1_URN,
        TEST_TERM_2_URN
    ), TermRelationshipType.isA);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
    verifyNoIngestProposal(mockService);
  }

}
