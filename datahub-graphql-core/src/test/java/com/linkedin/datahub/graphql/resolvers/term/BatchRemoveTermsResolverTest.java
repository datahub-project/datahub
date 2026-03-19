package com.linkedin.datahub.graphql.resolvers.term;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchRemoveTermsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchRemoveTermsResolver;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchRemoveTermsResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_TERM_1_URN = "urn:li:glossaryTerm:test-id-1";
  private static final String TEST_TERM_2_URN = "urn:li:glossaryTerm:test-id-2";

  @Test
  public void testGetSuccessNoExistingTerms() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getLatestAspects(
                any(),
                eq(Set.of(UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))),
                eq(Set.of(Constants.GLOSSARY_TERMS_ASPECT_NAME)),
                eq(false)))
        .thenReturn(null);

    Mockito.when(
            mockService.exists(
                any(),
                eq(
                    List.of(
                        Urn.createFromString(TEST_ENTITY_URN_1),
                        Urn.createFromString(TEST_ENTITY_URN_2))),
                eq(true)))
        .thenReturn(
            Set.of(
                Urn.createFromString(TEST_ENTITY_URN_1), Urn.createFromString(TEST_ENTITY_URN_2)));

    Mockito.when(
            mockService.exists(
                any(),
                eq(
                    List.of(
                        Urn.createFromString(TEST_TERM_1_URN),
                        Urn.createFromString(TEST_TERM_2_URN))),
                eq(true)))
        .thenReturn(
            Set.of(Urn.createFromString(TEST_TERM_1_URN), Urn.createFromString(TEST_TERM_2_URN)));

    BatchRemoveTermsResolver resolver = new BatchRemoveTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveTermsInput input =
        new BatchRemoveTermsInput(
            ImmutableList.of(TEST_TERM_1_URN, TEST_TERM_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetSuccessExistingTerms() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final GlossaryTerms oldTerms1 =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_1_URN)),
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_2_URN)))));

    final GlossaryTerms oldTerms2 =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_1_URN)))));

    Map<Urn, List<RecordTemplate>> result =
        Map.of(
            UrnUtils.getUrn(TEST_ENTITY_URN_1),
            List.of(oldTerms1),
            UrnUtils.getUrn(TEST_ENTITY_URN_2),
            List.of(oldTerms2));
    Mockito.when(
            mockService.getLatestAspects(
                any(),
                eq(Set.of(UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))),
                eq(Set.of(Constants.GLOSSARY_TERMS_ASPECT_NAME)),
                eq(false)))
        .thenReturn(result);

    Mockito.when(
            mockService.exists(
                any(),
                eq(
                    List.of(
                        Urn.createFromString(TEST_ENTITY_URN_1),
                        Urn.createFromString(TEST_ENTITY_URN_2))),
                eq(true)))
        .thenReturn(
            Set.of(
                Urn.createFromString(TEST_ENTITY_URN_1), Urn.createFromString(TEST_ENTITY_URN_2)));

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_2_URN)), eq(true)))
        .thenReturn(true);

    BatchRemoveTermsResolver resolver = new BatchRemoveTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveTermsInput input =
        new BatchRemoveTermsInput(
            ImmutableList.of(TEST_TERM_1_URN, TEST_TERM_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    verifyIngestProposal(mockService, 1);
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                eq(0L)))
        .thenReturn(null);
    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);

    BatchRemoveTermsResolver resolver = new BatchRemoveTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveTermsInput input =
        new BatchRemoveTermsInput(
            ImmutableList.of(TEST_TERM_1_URN, TEST_TERM_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    BatchRemoveTermsResolver resolver = new BatchRemoveTermsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchRemoveTermsInput input =
        new BatchRemoveTermsInput(
            ImmutableList.of(TEST_TERM_1_URN, TEST_TERM_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());

    BatchRemoveTermsResolver resolver = new BatchRemoveTermsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BatchRemoveTermsInput input =
        new BatchRemoveTermsInput(
            ImmutableList.of(TEST_TERM_1_URN, TEST_TERM_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
