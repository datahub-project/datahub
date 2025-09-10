package com.linkedin.datahub.graphql.resolvers.term;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
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
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.generated.TermAssociationInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveTermResolver;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaMetadata;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RemoveTermResolverTest {

  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_FIELD_PATH = "field0";
  private static final Urn TEST_SCHEMA_FIELD_URN =
      SchemaFieldUtils.generateSchemaFieldUrn(UrnUtils.getUrn(TEST_ENTITY_URN), TEST_FIELD_PATH);

  private static final String TEST_TERM_1_URN = "urn:li:glossaryTerm:test-id-1";
  private static final String TEST_TERM_2_URN = "urn:li:glossaryTerm:test-id-2";
  private static final String OLD_ACTOR_URN = "urn:li:corpuser:old";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final long OLD_TEST_TIME = 5L;
  private static final long TEST_TIME = 1234567890L;

  @Test
  public void testSuccessNoExistingTerms() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                eq(0L)))
        .thenReturn(null);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);

    // Execute resolver
    RemoveTermResolver resolver = new RemoveTermResolver(mockService);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TermAssociationInput input =
        new TermAssociationInput(TEST_TERM_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Have to mock supplyAsync so that EntityUtils static mock works
    try (MockedStatic<GraphQLConcurrencyUtils> mockGraphQLUtils =
            mockStatic(GraphQLConcurrencyUtils.class);
        MockedStatic<EntityUtils> mockedEntityUtils =
            mockStatic(EntityUtils.class, CALLS_REAL_METHODS)) {
      mockGraphQLUtils
          .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                Supplier<?> supplier = invocation.getArgument(0);
                return CompletableFuture.completedFuture(supplier.get());
              });
      mockedEntityUtils.when(EntityUtils::getTimestamp).thenReturn(TEST_TIME);
      assertTrue(resolver.get(mockEnv).get());
    }

    final GlossaryTerms emptyTerms =
        new GlossaryTerms()
            .setAuditStamp(
                new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(TEST_TIME))
            .setTerms(new GlossaryTermAssociationArray(Collections.emptyList()));

    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN),
            Constants.GLOSSARY_TERMS_ASPECT_NAME,
            emptyTerms);

    verifyIngestProposal(mockService, 1, List.of(proposal));
  }

  @Test
  public void testSuccessNoOp() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final GlossaryTerms oldTerms =
        new GlossaryTerms()
            .setAuditStamp(
                new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_2_URN))
                            .setActor(Urn.createFromString(OLD_ACTOR_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                eq(0L)))
        .thenReturn(oldTerms);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);

    // Execute resolver
    RemoveTermResolver resolver = new RemoveTermResolver(mockService);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TermAssociationInput input =
        new TermAssociationInput(TEST_TERM_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // Have to mock supplyAsync so that EntityUtils static mock works
    try (MockedStatic<GraphQLConcurrencyUtils> mockGraphQLUtils =
            mockStatic(GraphQLConcurrencyUtils.class);
        MockedStatic<EntityUtils> mockedEntityUtils =
            mockStatic(EntityUtils.class, CALLS_REAL_METHODS)) {
      mockGraphQLUtils
          .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                Supplier<?> supplier = invocation.getArgument(0);
                return CompletableFuture.completedFuture(supplier.get());
              });
      mockedEntityUtils.when(EntityUtils::getTimestamp).thenReturn(TEST_TIME);
      assertTrue(resolver.get(mockEnv).get());
    }

    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN), Constants.GLOSSARY_TERMS_ASPECT_NAME, oldTerms);

    verifyIngestProposal(mockService, 1, List.of(proposal));
  }

  @Test
  public void testSuccessExistingTerms() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final GlossaryTerms oldTerms =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_1_URN))
                            .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_2_URN))
                            .setActor(Urn.createFromString(OLD_ACTOR_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                eq(Constants.GLOSSARY_TERMS_ASPECT_NAME),
                eq(0L)))
        .thenReturn(oldTerms);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);

    RemoveTermResolver resolver = new RemoveTermResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TermAssociationInput input =
        new TermAssociationInput(TEST_TERM_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try (MockedStatic<GraphQLConcurrencyUtils> mockGraphQLUtils =
            mockStatic(GraphQLConcurrencyUtils.class);
        MockedStatic<EntityUtils> mockedEntityUtils =
            mockStatic(EntityUtils.class, CALLS_REAL_METHODS)) {
      mockGraphQLUtils
          .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                Supplier<?> supplier = invocation.getArgument(0);
                return CompletableFuture.completedFuture(supplier.get());
              });
      mockedEntityUtils.when(EntityUtils::getTimestamp).thenReturn(TEST_TIME);
      assertTrue(resolver.get(mockEnv).get());
    }

    final GlossaryTerms updatedTerms =
        new GlossaryTerms()
            .setAuditStamp(
                new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_2_URN))
                            .setActor(Urn.createFromString(OLD_ACTOR_URN)))));

    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN),
            Constants.GLOSSARY_TERMS_ASPECT_NAME,
            updatedTerms);

    verifyIngestProposal(mockService, 1, List.of(proposal));
  }

  @Test
  public void testGetSuccessSchemaField() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final SchemaMetadata oldSchema =
        new SchemaMetadata()
            .setFields(
                new SchemaFieldArray(
                    ImmutableList.of(
                        new SchemaField().setFieldPath(TEST_FIELD_PATH),
                        new SchemaField().setFieldPath("field0"))));

    final EditableSchemaMetadata oldEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(OLD_ACTOR_URN))
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_1_URN))
                                                    .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(OLD_ACTOR_URN))
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_1_URN))
                                                    .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldEditableSchema);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);

    RemoveTermResolver resolver = new RemoveTermResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TermAssociationInput input =
        new TermAssociationInput(
            TEST_TERM_1_URN, TEST_ENTITY_URN, SubResourceType.DATASET_FIELD, TEST_FIELD_PATH);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try (MockedStatic<GraphQLConcurrencyUtils> mockGraphQLUtils =
            mockStatic(GraphQLConcurrencyUtils.class);
        MockedStatic<EntityUtils> mockedEntityUtils =
            mockStatic(EntityUtils.class, CALLS_REAL_METHODS)) {
      mockGraphQLUtils
          .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                Supplier<?> supplier = invocation.getArgument(0);
                return CompletableFuture.completedFuture(supplier.get());
              });
      mockedEntityUtils.when(EntityUtils::getTimestamp).thenReturn(TEST_TIME);
      assertTrue(resolver.get(mockEnv).get());
    }

    final EditableSchemaMetadata newEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(TEST_ACTOR_URN))
                                            .setTime(TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(OLD_ACTOR_URN))
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_1_URN))
                                                    .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))))));

    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN),
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            newEditableSchema);

    verifyIngestProposal(mockService, 1, List.of(proposal));
  }

  @Test
  public void testGetSuccessSchemaFieldNoOp() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final SchemaMetadata oldSchema =
        new SchemaMetadata()
            .setFields(
                new SchemaFieldArray(
                    ImmutableList.of(new SchemaField().setFieldPath(TEST_FIELD_PATH))));

    final EditableSchemaMetadata oldEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(new EditableSchemaFieldInfo().setFieldPath(TEST_FIELD_PATH))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldEditableSchema);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);

    RemoveTermResolver resolver = new RemoveTermResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TermAssociationInput input =
        new TermAssociationInput(
            TEST_TERM_1_URN, TEST_ENTITY_URN, SubResourceType.DATASET_FIELD, TEST_FIELD_PATH);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try (MockedStatic<GraphQLConcurrencyUtils> mockGraphQLUtils =
            mockStatic(GraphQLConcurrencyUtils.class);
        MockedStatic<EntityUtils> mockedEntityUtils =
            mockStatic(EntityUtils.class, CALLS_REAL_METHODS)) {
      mockGraphQLUtils
          .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                Supplier<?> supplier = invocation.getArgument(0);
                return CompletableFuture.completedFuture(supplier.get());
              });
      mockedEntityUtils.when(EntityUtils::getTimestamp).thenReturn(TEST_TIME);
      assertTrue(resolver.get(mockEnv).get());
    }

    final EditableSchemaMetadata newEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(TEST_ACTOR_URN))
                                            .setTime(TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(ImmutableList.of()))))));

    final MetadataChangeProposal proposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN),
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            newEditableSchema);

    verifyIngestProposal(mockService, 1, List.of(proposal));
  }

  @Test
  public void testGetSuccessSchemaFieldWithGlossaryTermsAspect() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    final SchemaMetadata oldSchema =
        new SchemaMetadata()
            .setFields(
                new SchemaFieldArray(
                    ImmutableList.of(
                        new SchemaField().setFieldPath(TEST_FIELD_PATH),
                        new SchemaField().setFieldPath("field1"))));

    final EditableSchemaMetadata oldEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(OLD_ACTOR_URN))
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_1_URN))
                                                    .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(OLD_ACTOR_URN))
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_1_URN))
                                                    .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))))));

    final GlossaryTerms oldTerms =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_1_URN))
                            .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_2_URN))
                            .setActor(Urn.createFromString(OLD_ACTOR_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldEditableSchema);
    Mockito.when(
            mockService.getAspect(
                any(), eq(TEST_SCHEMA_FIELD_URN), eq(Constants.GLOSSARY_TERMS_ASPECT_NAME), eq(0L)))
        .thenReturn(oldTerms);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(TEST_SCHEMA_FIELD_URN), eq(true))).thenReturn(true);

    RemoveTermResolver resolver = new RemoveTermResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TermAssociationInput input =
        new TermAssociationInput(
            TEST_TERM_1_URN, TEST_ENTITY_URN, SubResourceType.DATASET_FIELD, TEST_FIELD_PATH);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try (MockedStatic<GraphQLConcurrencyUtils> mockGraphQLUtils =
            mockStatic(GraphQLConcurrencyUtils.class);
        MockedStatic<EntityUtils> mockedEntityUtils =
            mockStatic(EntityUtils.class, CALLS_REAL_METHODS)) {
      mockGraphQLUtils
          .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                Supplier<?> supplier = invocation.getArgument(0);
                return CompletableFuture.completedFuture(supplier.get());
              });
      mockedEntityUtils.when(EntityUtils::getTimestamp).thenReturn(TEST_TIME);
      assertTrue(resolver.get(mockEnv).get());
    }

    final EditableSchemaMetadata newEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(TEST_ACTOR_URN))
                                            .setTime(TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(OLD_ACTOR_URN))
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_1_URN))
                                                    .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))))));

    final GlossaryTerms updatedTerms =
        new GlossaryTerms()
            .setAuditStamp(
                new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_2_URN))
                            .setActor(Urn.createFromString(OLD_ACTOR_URN)))));

    final MetadataChangeProposal editableSchemaMetadataProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN),
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            newEditableSchema);

    final MetadataChangeProposal glossaryTermsProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            TEST_SCHEMA_FIELD_URN, Constants.GLOSSARY_TERMS_ASPECT_NAME, updatedTerms);

    verifyIngestProposal(
        mockService, 1, List.of(glossaryTermsProposal, editableSchemaMetadataProposal));
  }

  @Test
  public void testGetSuccessSchemaFieldUrn() throws Exception {
    EntityService<?> mockService = getMockEntityService();
    String TEST_FIELD_PATH = "field0";

    final SchemaMetadata oldSchema =
        new SchemaMetadata()
            .setFields(
                new SchemaFieldArray(
                    ImmutableList.of(
                        new SchemaField().setFieldPath(TEST_FIELD_PATH),
                        new SchemaField().setFieldPath("field1"))));

    final EditableSchemaMetadata oldEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(OLD_ACTOR_URN))
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_1_URN))
                                                    .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(OLD_ACTOR_URN))
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_1_URN))
                                                    .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))))));

    final GlossaryTerms oldTerms =
        new GlossaryTerms()
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_1_URN))
                            .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_2_URN))
                            .setActor(Urn.createFromString(OLD_ACTOR_URN)))));

    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldSchema);
    Mockito.when(
            mockService.getAspect(
                any(),
                Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN)),
                Mockito.eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME),
                Mockito.eq(0L)))
        .thenReturn(oldEditableSchema);
    Mockito.when(
            mockService.getAspect(
                any(), eq(TEST_SCHEMA_FIELD_URN), eq(Constants.GLOSSARY_TERMS_ASPECT_NAME), eq(0L)))
        .thenReturn(oldTerms);

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(TEST_SCHEMA_FIELD_URN), eq(true))).thenReturn(true);

    RemoveTermResolver resolver = new RemoveTermResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TermAssociationInput input =
        new TermAssociationInput(TEST_TERM_1_URN, TEST_SCHEMA_FIELD_URN.toString(), null, null);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    try (MockedStatic<GraphQLConcurrencyUtils> mockGraphQLUtils =
            mockStatic(GraphQLConcurrencyUtils.class);
        MockedStatic<EntityUtils> mockedEntityUtils =
            mockStatic(EntityUtils.class, CALLS_REAL_METHODS)) {
      mockGraphQLUtils
          .when(() -> GraphQLConcurrencyUtils.supplyAsync(any(), any(), any()))
          .thenAnswer(
              invocation -> {
                Supplier<?> supplier = invocation.getArgument(0);
                return CompletableFuture.completedFuture(supplier.get());
              });
      mockedEntityUtils.when(EntityUtils::getTimestamp).thenReturn(TEST_TIME);
      assertTrue(resolver.get(mockEnv).get());
    }

    final EditableSchemaMetadata newEditableSchema =
        new EditableSchemaMetadata()
            .setEditableSchemaFieldInfo(
                new EditableSchemaFieldInfoArray(
                    ImmutableList.of(
                        new EditableSchemaFieldInfo()
                            .setFieldPath(TEST_FIELD_PATH)
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(TEST_ACTOR_URN))
                                            .setTime(TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))),
                        new EditableSchemaFieldInfo()
                            .setFieldPath("field1")
                            .setGlossaryTerms(
                                new GlossaryTerms()
                                    .setAuditStamp(
                                        new AuditStamp()
                                            .setActor(Urn.createFromString(OLD_ACTOR_URN))
                                            .setTime(OLD_TEST_TIME))
                                    .setTerms(
                                        new GlossaryTermAssociationArray(
                                            ImmutableList.of(
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_1_URN))
                                                    .setActor(Urn.createFromString(OLD_ACTOR_URN)),
                                                new GlossaryTermAssociation()
                                                    .setUrn(
                                                        GlossaryTermUrn.createFromString(
                                                            TEST_TERM_2_URN))
                                                    .setActor(
                                                        Urn.createFromString(OLD_ACTOR_URN)))))))));

    final GlossaryTerms updatedTerms =
        new GlossaryTerms()
            .setAuditStamp(
                new AuditStamp().setActor(Urn.createFromString(TEST_ACTOR_URN)).setTime(TEST_TIME))
            .setTerms(
                new GlossaryTermAssociationArray(
                    ImmutableList.of(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromString(TEST_TERM_2_URN))
                            .setActor(Urn.createFromString(OLD_ACTOR_URN)))));

    final MetadataChangeProposal editableSchemaMetadataProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN),
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            newEditableSchema);

    final MetadataChangeProposal glossaryTermsProposal =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            TEST_SCHEMA_FIELD_URN, Constants.GLOSSARY_TERMS_ASPECT_NAME, updatedTerms);

    verifyIngestProposal(
        mockService, 1, List.of(glossaryTermsProposal, editableSchemaMetadataProposal));
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(false);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);

    RemoveTermResolver resolver = new RemoveTermResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TermAssociationInput input =
        new TermAssociationInput(TEST_TERM_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  @Test
  public void testGetFailureTermDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(false);

    RemoveTermResolver resolver = new RemoveTermResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TermAssociationInput input =
        new TermAssociationInput(TEST_TERM_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    RemoveTermResolver resolver = new RemoveTermResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    TermAssociationInput input =
        new TermAssociationInput(TEST_TERM_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(AuthorizationException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    // We need to mock exists calls first so validation doesn't fail first
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN)), eq(true)))
        .thenReturn(true);
    Mockito.when(mockService.exists(any(), eq(Urn.createFromString(TEST_TERM_1_URN)), eq(true)))
        .thenReturn(true);

    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());

    RemoveTermResolver resolver = new RemoveTermResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    TermAssociationInput input =
        new TermAssociationInput(TEST_TERM_1_URN, TEST_ENTITY_URN, null, null);
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }
}
