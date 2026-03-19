package com.linkedin.datahub.graphql.resolvers.tag;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.businessattribute.BusinessAttributeInfo;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BatchAddTagsInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.datahub.graphql.resolvers.mutate.BatchAddTagsResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.datahub.graphql.resolvers.mutate.util.BusinessAttributeUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.*;
import graphql.schema.DataFetchingEnvironment;
import java.util.*;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class BatchAddTagsResolverTest {

  private static final String TEST_ENTITY_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)";
  private static final String TEST_ENTITY_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)";
  private static final String TEST_TAG_1_URN = "urn:li:tag:test-id-1";
  private static final String TEST_TAG_2_URN = "urn:li:tag:test-id-2";
  private static final String TEST_BUSINESS_ATTRIBUTE_URN_2 =
      "urn:li:businessAttribute:7d0c4283-de02-4043-aaf2-698b04274658";
  private static final String TEST_BUSINESS_ATTRIBUTE_URN_1 =
      "urn:li:businessAttribute:7d0c4283-de02-4043-aaf2-698b04274659";

  @Test
  public void testGetSuccessNoExistingTags() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    when(mockService.getAspect(
            any(),
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
            Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(null);

    when(mockService.getAspect(
            any(),
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
            Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(null);

    // Mock batch exists() calls for validation - return all requested URNs as existing
    when(mockService.exists(any(), any(java.util.Collection.class), eq(true)))
        .thenAnswer(
            invocation -> {
              java.util.Collection<Urn> urns = invocation.getArgument(1);
              return new java.util.HashSet<>(urns);
            });

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags newTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_1), GLOBAL_TAGS_ASPECT_NAME, newTags);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_2), GLOBAL_TAGS_ASPECT_NAME, newTags);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));

    verify(mockService, times(1))
        .exists(
            any(),
            Mockito.eq(
                List.of(
                    Urn.createFromString(TEST_TAG_1_URN), Urn.createFromString(TEST_TAG_2_URN))),
            eq(true));
  }

  @Test
  public void testGetSuccessExistingTags() throws Exception {
    GlobalTags originalTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)))));

    EntityService<?> mockService = getMockEntityService();

    when(mockService.getLatestAspects(
            any(),
            Mockito.eq(
                Set.of(UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))),
            Mockito.eq(Set.of(Constants.GLOBAL_TAGS_ASPECT_NAME)),
            Mockito.eq(false)))
        .thenReturn(new HashMap<>());

    when(mockService.getAspect(
            any(),
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
            Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(originalTags);

    // Mock batch exists() calls for validation - return all requested URNs as existing
    when(mockService.exists(any(), any(java.util.Collection.class), eq(true)))
        .thenAnswer(
            invocation -> {
              java.util.Collection<Urn> urns = invocation.getArgument(1);
              return new java.util.HashSet<>(urns);
            });

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags newTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_1), GLOBAL_TAGS_ASPECT_NAME, newTags);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_2), GLOBAL_TAGS_ASPECT_NAME, newTags);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));

    verify(mockService, times(1))
        .exists(
            any(),
            Mockito.eq(
                List.of(
                    Urn.createFromString(TEST_TAG_1_URN), Urn.createFromString(TEST_TAG_2_URN))),
            eq(true));
  }

  @Test
  public void testGetSuccessExistingTagsWithBusinessAttributes() throws Exception {
    Urn urn1 = UrnUtils.getUrn(TEST_BUSINESS_ATTRIBUTE_URN_1);
    Urn urn2 = UrnUtils.getUrn(TEST_BUSINESS_ATTRIBUTE_URN_2);
    GlobalTags originalTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)))));
    BusinessAttributeInfo info = new BusinessAttributeInfo();
    info.setFieldPath("test-business-attribute-updated");
    info.setName("test-business-attribute");
    info.setDescription("ttest-description");
    info.setType(
        BusinessAttributeUtils.mapSchemaFieldDataType(
            com.linkedin.datahub.graphql.generated.SchemaFieldDataType.BOOLEAN),
        SetMode.IGNORE_NULL);

    EntityService<?> mockService = getMockEntityService();

    Map<Urn, List<RecordTemplate>> tagResult =
        Map.of(urn1, List.of(originalTags), urn2, List.of(originalTags));
    Map<Urn, List<RecordTemplate>> businessAttrResult =
        Map.of(urn1, List.of(info, info), urn2, List.of(info));
    when(mockService.getLatestAspects(
            any(),
            Mockito.eq(Set.of(urn1, urn2)),
            Mockito.eq(Set.of(Constants.GLOBAL_TAGS_ASPECT_NAME)),
            Mockito.eq(false)))
        .thenReturn(tagResult);

    when(mockService.getLatestAspects(
            any(),
            Mockito.eq(Set.of(urn1, urn2)),
            Mockito.eq(Set.of(BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME)),
            Mockito.eq(false)))
        .thenReturn(businessAttrResult);

    // Mock batch exists() calls for validation - return all requested URNs as existing
    when(mockService.exists(
            any(),
            eq(List.of(Urn.createFromString(TEST_TAG_1_URN), Urn.createFromString(TEST_TAG_2_URN))),
            eq(true)))
        .thenReturn(
            Set.of(Urn.createFromString(TEST_TAG_1_URN), Urn.createFromString(TEST_TAG_2_URN)));
    when(mockService.exists(any(), eq(List.of(urn1, urn2)), eq(true)))
        .thenReturn(Set.of(urn1, urn2));
    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_BUSINESS_ATTRIBUTE_URN_1, null, null),
                new ResourceRefInput(TEST_BUSINESS_ATTRIBUTE_URN_2, null, null)));
    when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags newTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(urn1, GLOBAL_TAGS_ASPECT_NAME, newTags);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(urn2, GLOBAL_TAGS_ASPECT_NAME, newTags);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));

    verify(mockService, times(1)).exists(any(), Mockito.eq(List.of(urn1, urn2)), eq(true));
  }

  @Test
  public void testGetSuccessExistingTagsWithSubresource() throws Exception {
    GlobalTags originalTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)))));

    EntityService<?> mockService = getMockEntityService();

    SchemaMetadata schema_1 = new SchemaMetadata();
    SchemaField field1 =
        new SchemaField()
            .setFieldPath("field_bar")
            .setDescription("test_des")
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNativeDataType("string");
    SchemaField field2 =
        new SchemaField()
            .setFieldPath("field_foo")
            .setDescription("test_des")
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNativeDataType("string");
    schema_1.setFields(new SchemaFieldArray(field1, field2));

    SchemaMetadata schema_2 = new SchemaMetadata();
    SchemaField field3 =
        new SchemaField()
            .setFieldPath("field_bar")
            .setDescription("test_des")
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNativeDataType("string");
    schema_2.setFields(new SchemaFieldArray(field3));

    Map<Urn, List<RecordTemplate>> subResourceResult =
        Map.of(
            UrnUtils.getUrn(TEST_ENTITY_URN_1),
            List.of(schema_1),
            UrnUtils.getUrn(TEST_ENTITY_URN_2),
            List.of(schema_2));
    Map<Urn, List<RecordTemplate>> tagResult =
        Map.of(
            UrnUtils.getUrn(TEST_ENTITY_URN_2),
            List.of(originalTags),
            UrnUtils.getUrn(TEST_ENTITY_URN_1),
            new ArrayList<>());
    when(mockService.getLatestAspects(
            any(),
            Mockito.eq(
                Set.of(UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))),
            Mockito.eq(Set.of(Constants.GLOBAL_TAGS_ASPECT_NAME)),
            Mockito.eq(false)))
        .thenReturn(tagResult);

    when(mockService.getLatestAspects(
            any(),
            Mockito.eq(
                Set.of(UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))),
            Mockito.eq(Set.of(SCHEMA_METADATA_ASPECT_NAME)),
            Mockito.eq(false)))
        .thenReturn(subResourceResult);

    // Mock batch exists() calls for validation - return all requested URNs as existing
    when(mockService.exists(any(), any(Collection.class), eq(true)))
        .thenAnswer(
            invocation -> {
              Collection<Urn> urns = invocation.getArgument(1);
              return new HashSet<>(urns);
            });

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "field_bar"),
                new ResourceRefInput(TEST_ENTITY_URN_1, SubResourceType.DATASET_FIELD, "field_foo"),
                new ResourceRefInput(
                    TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "field_bar")));
    when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    final GlobalTags newTags =
        new GlobalTags()
            .setTags(
                new TagAssociationArray(
                    ImmutableList.of(
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_1_URN)),
                        new TagAssociation().setTag(TagUrn.createFromString(TEST_TAG_2_URN)))));

    final MetadataChangeProposal proposal1 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_1), GLOBAL_TAGS_ASPECT_NAME, newTags);
    final MetadataChangeProposal proposal2 =
        MutationUtils.buildMetadataChangeProposalWithUrn(
            Urn.createFromString(TEST_ENTITY_URN_2), GLOBAL_TAGS_ASPECT_NAME, newTags);

    verifyIngestProposal(mockService, 1, List.of(proposal1, proposal2));

    verify(mockService, times(1))
        .getLatestAspects(
            any(),
            Mockito.eq(
                Set.of(UrnUtils.getUrn(TEST_ENTITY_URN_1), UrnUtils.getUrn(TEST_ENTITY_URN_2))),
            Mockito.eq(Set.of(SCHEMA_METADATA_ASPECT_NAME)),
            Mockito.eq(false));
    verify(mockService, times(1))
        .exists(
            any(),
            Mockito.eq(
                List.of(
                    Urn.createFromString(TEST_TAG_1_URN), Urn.createFromString(TEST_TAG_2_URN))),
            eq(true));
  }

  @Test
  public void testGetFailureTagDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    when(mockService.getAspect(
            any(),
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
            Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(null);

    when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(true);
    when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(false);

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verify(mockService, times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  @Test
  public void testGetFailureResourceDoesNotExist() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    when(mockService.getAspect(
            any(),
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_1)),
            Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(null);
    when(mockService.getAspect(
            any(),
            Mockito.eq(UrnUtils.getUrn(TEST_ENTITY_URN_2)),
            Mockito.eq(Constants.GLOBAL_TAGS_ASPECT_NAME),
            Mockito.eq(0L)))
        .thenReturn(null);

    when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_1)), eq(true)))
        .thenReturn(false);
    when(mockService.exists(any(), eq(Urn.createFromString(TEST_ENTITY_URN_2)), eq(true)))
        .thenReturn(true);
    when(mockService.exists(any(), eq(Urn.createFromString(TEST_TAG_1_URN)), eq(true)))
        .thenReturn(true);

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verify(mockService, times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                new ResourceRefInput(TEST_ENTITY_URN_2, null, null)));
    when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    QueryContext mockContext = getMockDenyContext();
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    verify(mockService, times(0))
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());
  }

  @Test
  public void testGetEntityClientException() throws Exception {
    EntityService<?> mockService = getMockEntityService();

    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .ingestProposal(any(), Mockito.any(AspectsBatchImpl.class), Mockito.anyBoolean());

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN),
            ImmutableList.of(new ResourceRefInput(TEST_ENTITY_URN_1, null, null)));
    when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  @Test
  public void testGetSuccessMixedResourceTypes() throws Exception {
    // Comprehensive test: regular dataset + business attributes + schema fields in single request
    Urn datasetUrn = UrnUtils.getUrn(TEST_ENTITY_URN_1);
    Urn businessAttrUrn1 = UrnUtils.getUrn(TEST_BUSINESS_ATTRIBUTE_URN_1);
    Urn businessAttrUrn2 = UrnUtils.getUrn(TEST_BUSINESS_ATTRIBUTE_URN_2);
    Urn datasetUrn2 = UrnUtils.getUrn(TEST_ENTITY_URN_2);

    // Setup BusinessAttributeInfo with required fields
    BusinessAttributeInfo info = new BusinessAttributeInfo();
    info.setFieldPath("test-business-attribute");
    info.setName("test-business-attribute");
    info.setDescription("test-description");
    info.setType(
        BusinessAttributeUtils.mapSchemaFieldDataType(
            com.linkedin.datahub.graphql.generated.SchemaFieldDataType.STRING),
        SetMode.IGNORE_NULL);

    // Setup SchemaMetadata with fields
    SchemaMetadata schema = new SchemaMetadata();
    SchemaField field1 =
        new SchemaField()
            .setFieldPath("column1")
            .setDescription("test column")
            .setType(
                new SchemaFieldDataType()
                    .setType(SchemaFieldDataType.Type.create(new StringType())))
            .setNativeDataType("string");
    schema.setFields(new SchemaFieldArray(field1));

    EntityService<?> mockService = getMockEntityService();

    // Mock tag aspects
    Map<Urn, List<RecordTemplate>> tagResult =
        Map.of(
            datasetUrn, List.of(new GlobalTags()),
            businessAttrUrn1, List.of(new GlobalTags()),
            businessAttrUrn2, List.of(new GlobalTags()),
            datasetUrn2, List.of(new GlobalTags()));

    when(mockService.getLatestAspects(
            any(),
            Mockito.eq(Set.of(datasetUrn, businessAttrUrn1, businessAttrUrn2)),
            Mockito.eq(Set.of(Constants.GLOBAL_TAGS_ASPECT_NAME)),
            Mockito.eq(false)))
        .thenReturn(tagResult);

    // Mock business attribute aspects
    Map<Urn, List<RecordTemplate>> businessAttrResult =
        Map.of(
            businessAttrUrn1, List.of(info),
            businessAttrUrn2, List.of(info));

    when(mockService.getLatestAspects(
            any(),
            Mockito.eq(Set.of(businessAttrUrn1, businessAttrUrn2)),
            Mockito.eq(Set.of(BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME)),
            Mockito.eq(false)))
        .thenReturn(businessAttrResult);

    // Mock schema aspects for subresources
    Map<Urn, List<RecordTemplate>> schemaResult = Map.of(datasetUrn2, List.of(schema));

    when(mockService.getLatestAspects(
            any(),
            Mockito.eq(Set.of(datasetUrn2)),
            Mockito.eq(Set.of(SCHEMA_METADATA_ASPECT_NAME)),
            Mockito.eq(false)))
        .thenReturn(schemaResult);

    // Mock batch exists() calls
    when(mockService.exists(any(), any(Collection.class), eq(true)))
        .thenAnswer(
            invocation -> {
              Collection<Urn> urns = invocation.getArgument(1);
              return new HashSet<>(urns);
            });

    BatchAddTagsResolver resolver = new BatchAddTagsResolver(mockService);

    // Execute resolver with mixed resource types
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    BatchAddTagsInput input =
        new BatchAddTagsInput(
            ImmutableList.of(TEST_TAG_1_URN, TEST_TAG_2_URN),
            ImmutableList.of(
                // Regular dataset entity
                new ResourceRefInput(TEST_ENTITY_URN_1, null, null),
                // Business attributes
                new ResourceRefInput(TEST_BUSINESS_ATTRIBUTE_URN_1, null, null),
                new ResourceRefInput(TEST_BUSINESS_ATTRIBUTE_URN_2, null, null),
                // Schema field subresources
                new ResourceRefInput(TEST_ENTITY_URN_2, SubResourceType.DATASET_FIELD, "column1")));
    when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    // Verify batch operations were used
    verify(mockService, times(1))
        .exists(
            any(),
            Mockito.eq(
                List.of(
                    Urn.createFromString(TEST_TAG_1_URN), Urn.createFromString(TEST_TAG_2_URN))),
            eq(true));

    verify(mockService, times(1))
        .getLatestAspects(
            any(),
            Mockito.eq(Set.of(datasetUrn, businessAttrUrn1, businessAttrUrn2)),
            Mockito.eq(Set.of(Constants.GLOBAL_TAGS_ASPECT_NAME)),
            Mockito.eq(false));

    verify(mockService, times(1))
        .getLatestAspects(
            any(),
            Mockito.eq(Set.of(datasetUrn2)),
            Mockito.eq(Set.of(SCHEMA_METADATA_ASPECT_NAME)),
            Mockito.eq(false));
  }
}
