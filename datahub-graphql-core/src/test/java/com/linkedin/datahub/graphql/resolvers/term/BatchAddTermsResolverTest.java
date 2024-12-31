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
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchAddTermsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchAddTermsResolver;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchAddTermsResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_GLOSSARY_TERM_1_URN = "urn:li:glossaryTerm:test-id-1";
  private static final String TEST_GLOSSARY_TERM_2_URN = "urn:li:glossaryTerm:test-id-2";

  @Test
  public void testGetSuccessNoExistingTerms() throws Exception {
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
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);

    Mockito.when(
            mockService.exists(any(), eq(Urn.createFromString(TEST_GLOSSARY_TERM_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(any(), eq(Urn.createFromString(TEST_GLOSSARY_TERM_2_URN)), eq(true)))
        .thenReturn(true);

    BatchAddTermsResolver resolver = new BatchAddTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTermsInput input =
        new BatchAddTermsInput(
            ImmutableList.of(TEST_GLOSSARY_TERM_1_URN, TEST_GLOSSARY_TERM_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    verifyIngestProposal(mockService, 1);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_GLOSSARY_TERM_1_URN)), eq(true));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_GLOSSARY_TERM_2_URN)), eq(true));
  }

  @Test
  public void testGetSuccessExistingTerms() throws Exception {
    GlossaryTerms originalTerms =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_GLOSSARY_TERM_1_URN)))));

    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                eq(0L)))
        .thenReturn(originalTerms);

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
                eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                eq(0L)))
        .thenReturn(originalTerms);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);

    Mockito.when(
            mockService.exists(any(), eq(Urn.createFromString(TEST_GLOSSARY_TERM_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(any(), eq(Urn.createFromString(TEST_GLOSSARY_TERM_2_URN)), eq(true)))
        .thenReturn(true);

    BatchAddTermsResolver resolver = new BatchAddTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTermsInput input =
        new BatchAddTermsInput(
            ImmutableList.of(TEST_GLOSSARY_TERM_1_URN, TEST_GLOSSARY_TERM_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    verifyIngestProposal(mockService, 1);

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_GLOSSARY_TERM_1_URN)), eq(true));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(any(), eq(Urn.createFromString(TEST_GLOSSARY_TERM_2_URN)), eq(true));
  }

  @Test
  public void testGetFailureTagDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
                eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    Mockito.when(
            mockService.exists(any(), eq(Urn.createFromString(TEST_GLOSSARY_TERM_1_URN)), eq(true)))
        .thenReturn(false);

    BatchAddTermsResolver resolver = new BatchAddTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTermsInput input =
        new BatchAddTermsInput(
            ImmutableList.of(TEST_GLOSSARY_TERM_1_URN, TEST_GLOSSARY_TERM_2_URN),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verifyNoIngestProposal(mockService);
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
    Mockito.when(
            mockService.exists(any(), eq(Urn.createFromString(TEST_GLOSSARY_TERM_1_URN)), eq(true)))
        .thenReturn(true);

    BatchAddTermsResolver resolver = new BatchAddTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTermsInput input =
        new BatchAddTermsInput(
            ImmutableList.of(TEST_GLOSSARY_TERM_1_URN, TEST_GLOSSARY_TERM_2_URN),
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

    BatchAddTermsResolver resolver = new BatchAddTermsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTermsInput input =
        new BatchAddTermsInput(
            ImmutableList.of(TEST_GLOSSARY_TERM_1_URN, TEST_GLOSSARY_TERM_2_URN),
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

    BatchAddTermsResolver resolver = new BatchAddTermsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BatchAddTermsInput input =
        new BatchAddTermsInput(
            ImmutableList.of(TEST_GLOSSARY_TERM_1_URN),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
