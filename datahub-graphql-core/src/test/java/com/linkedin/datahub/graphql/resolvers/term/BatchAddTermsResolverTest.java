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
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchAddTermsResolver;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
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

    stubExistingUrns(
        mockService,
        Urn.createFromString(TEST_ENTITY_URN_1),
        Urn.createFromString(TEST_ENTITY_URN_2),
        Urn.createFromString(TEST_GLOSSARY_TERM_1_URN),
        Urn.createFromString(TEST_GLOSSARY_TERM_2_URN));

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

    verifyExistenceResolvedInBatches(mockService, 2);
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

    stubExistingUrns(
        mockService,
        Urn.createFromString(TEST_ENTITY_URN_1),
        Urn.createFromString(TEST_ENTITY_URN_2),
        Urn.createFromString(TEST_GLOSSARY_TERM_1_URN),
        Urn.createFromString(TEST_GLOSSARY_TERM_2_URN));

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

    verifyExistenceResolvedInBatches(mockService, 2);
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

    // The first term does not exist.
    stubExistingUrns(
        mockService,
        Urn.createFromString(TEST_ENTITY_URN_1),
        Urn.createFromString(TEST_GLOSSARY_TERM_2_URN));

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

    // Both terms resolve, but the first resource does not.
    stubExistingUrns(
        mockService,
        Urn.createFromString(TEST_ENTITY_URN_2),
        Urn.createFromString(TEST_GLOSSARY_TERM_1_URN),
        Urn.createFromString(TEST_GLOSSARY_TERM_2_URN));

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
  public void testGetSuccessTwoColumnsOnOneDataset() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    Urn datasetUrn = UrnUtils.getUrn(TEST_ENTITY_URN_1);

    Mockito.when(
            mockService.getAspect(
                any(), eq(datasetUrn), eq(Constants.SCHEMA_METADATA_ASPECT_NAME), eq(0L)))
        .thenReturn(
            new SchemaMetadata()
                .setFields(
                    new SchemaFieldArray(
                        ImmutableList.of(
                            new SchemaField().setFieldPath("col_a"),
                            new SchemaField().setFieldPath("col_b")))));

    stubExistingUrns(mockService, datasetUrn, Urn.createFromString(TEST_GLOSSARY_TERM_1_URN));

    BatchAddTermsResolver resolver = new BatchAddTermsResolver(mockService);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTermsInput input =
        new BatchAddTermsInput(
            ImmutableList.of(TEST_GLOSSARY_TERM_1_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "col_a"),
                new ResourceRefInput(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "col_b")));
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    ArgumentCaptor<AspectsBatchImpl> batchCaptor = ArgumentCaptor.forClass(AspectsBatchImpl.class);
    Mockito.verify(mockService, Mockito.times(1))
        .ingestProposal(any(), batchCaptor.capture(), eq(false));

    // Both columns must land in a single write. Two writes of the whole aspect would each
    // carry one column, and the second would drop the first.
    List<BatchItem> items = List.copyOf(batchCaptor.getValue().getItems());
    assertEquals(items.size(), 1);
    assertEquals(items.get(0).getUrn(), datasetUrn);
    EditableSchemaMetadata written = items.get(0).getAspect(EditableSchemaMetadata.class);
    assertEquals(written.getEditableSchemaFieldInfo().size(), 2);
    for (EditableSchemaFieldInfo fieldInfo : written.getEditableSchemaFieldInfo()) {
      assertEquals(
          fieldInfo.getGlossaryTerms().getTerms().get(0).getUrn().toString(),
          TEST_GLOSSARY_TERM_1_URN);
    }
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
