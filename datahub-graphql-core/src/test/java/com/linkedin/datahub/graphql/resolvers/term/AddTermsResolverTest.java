package com.linkedin.datahub.graphql.resolvers.term;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AddTermsInput;
import com.linkedin.datahub.graphql.resolvers.mutate.AddTermsResolver;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AddTermsResolverTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_TERM_1_URN = "urn:li:glossaryTerm:test-id-1";
  private static final String TEST_TERM_2_URN = "urn:li:glossaryTerm:test-id-2";

  @Test
  public void testGetSuccessNoExistingTerms() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_2_URN))).thenReturn(true);

    AddTermsResolver resolver = new AddTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddTermsInput input =
        new AddTermsInput(
            ImmutableList.of(TEST_TERM_1_URN, TEST_TERM_2_URN), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    // Unable to easily validate exact payload due to the injected timestamp
    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class), Mockito.any(AuditStamp.class), Mockito.eq(false));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_TERM_1_URN)));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_TERM_2_URN)));
  }

  @Test
  public void testGetSuccessExistingTerms() throws Exception {
    GlossaryTerms originalTerms =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_1_URN)))));

    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(originalTerms);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_1_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_2_URN))).thenReturn(true);

    AddTermsResolver resolver = new AddTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddTermsInput input =
        new AddTermsInput(
            ImmutableList.of(TEST_TERM_1_URN, TEST_TERM_2_URN), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    // Unable to easily validate exact payload due to the injected timestamp
    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class), Mockito.any(AuditStamp.class), Mockito.eq(false));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_TERM_1_URN)));

    Mockito.verify(mockService, Mockito.times(1))
        .exists(Mockito.eq(Urn.createFromString(TEST_TERM_2_URN)));
  }

  @Test
  public void testGetFailureTermDoesNotExist() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(true);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_1_URN))).thenReturn(false);

    AddTermsResolver resolver = new AddTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddTermsInput input =
        new AddTermsInput(ImmutableList.of(TEST_TERM_1_URN), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(Urn.createFromString(TEST_ENTITY_URN))).thenReturn(false);
    Mockito.when(mockService.exists(Urn.createFromString(TEST_TERM_1_URN))).thenReturn(true);

    AddTermsResolver resolver = new AddTermsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddTermsInput input =
        new AddTermsInput(ImmutableList.of(TEST_TERM_1_URN), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService mockService = getMockEntityService();

    AddTermsResolver resolver = new AddTermsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    AddTermsInput input =
        new AddTermsInput(ImmutableList.of(TEST_TERM_1_URN), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService mockService = getMockEntityService();

    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .ingestProposal(
            Mockito.any(AspectsBatchImpl.class),
            Mockito.any(AuditStamp.class),
            Mockito.anyBoolean());

    AddTermsResolver resolver = new AddTermsResolver(Mockito.mock(EntityService.class));

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    AddTermsInput input =
        new AddTermsInput(ImmutableList.of(TEST_TERM_1_URN), TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
