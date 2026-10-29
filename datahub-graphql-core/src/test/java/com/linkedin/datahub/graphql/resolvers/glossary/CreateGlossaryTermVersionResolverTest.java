package com.linkedin.datahub.graphql.resolvers.glossary;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.Status;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.CreateGlossaryTermVersionInput;
import com.linkedin.domain.Domains;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.lifecycle.LifecycleStageSettings;
import com.linkedin.lifecycle.LifecycleStageTransitionPolicy;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.versioning.EntityVersioningService;
import com.linkedin.metadata.entity.versioning.VersionPropertiesInput;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.service.LifecycleStages;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.versionset.VersionSetProperties;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class CreateGlossaryTermVersionResolverTest {

  private static final String TEST_TERM_URN = "urn:li:glossaryTerm:Revenue_v3";
  private static final String TEST_VERSION_SET_URN = "urn:li:versionSet:(kpi-revenue,glossaryTerm)";
  private static final String TEST_PREV_TERM_URN = "urn:li:glossaryTerm:Revenue_v2";

  @Test
  public void testGetSuccessful_withVersionSetUrn() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = getMockEntityService();
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);
    Mockito.when(mockClient.exists(any(), any())).thenReturn(false);
    Mockito.when(mockClient.ingestProposal(any(), any(), anyBoolean())).thenReturn(TEST_TERM_URN);

    IngestResult mockResult =
        IngestResult.builder().urn(Urn.createFromString(TEST_TERM_URN)).build();
    Mockito.when(
            mockVersioningService.linkLatestVersion(
                any(),
                eq(UrnUtils.getUrn(TEST_VERSION_SET_URN)),
                eq(UrnUtils.getUrn(TEST_TERM_URN)),
                any(VersionPropertiesInput.class)))
        .thenReturn(ImmutableList.of(mockResult));

    CreateGlossaryTermVersionResolver resolver =
        new CreateGlossaryTermVersionResolver(
            mockClient, mockService, mockVersioningService, mockFlags);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    CreateGlossaryTermVersionInput input = new CreateGlossaryTermVersionInput();
    input.setName("Revenue v3");
    input.setDescription("Updated definition.");
    input.setVersion("v3");
    input.setVersionSet(TEST_VERSION_SET_URN);
    input.setComment("Q3 policy update.");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_VERSION_SET_URN);

    // linkLatestVersion should be called once with the correct version set
    Mockito.verify(mockVersioningService, Mockito.times(1))
        .linkLatestVersion(
            any(),
            eq(UrnUtils.getUrn(TEST_VERSION_SET_URN)),
            eq(UrnUtils.getUrn(TEST_TERM_URN)),
            any(VersionPropertiesInput.class));
  }

  @Test
  public void testGetSuccessful_withPreviousTermUrn() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = getMockEntityService();
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);
    Mockito.when(mockClient.exists(any(), any())).thenReturn(false);
    Mockito.when(mockClient.ingestProposal(any(), any(), anyBoolean())).thenReturn(TEST_TERM_URN);

    // Mock getV2 to return VersionProperties for the previous term
    com.linkedin.common.VersionProperties vp = new com.linkedin.common.VersionProperties();
    vp.setVersionSet(UrnUtils.getUrn(TEST_VERSION_SET_URN));
    vp.setVersion(new com.linkedin.common.VersionTag().setVersionTag("v2"));
    vp.setSortId("AAAAAAAB");
    vp.setVersioningScheme(
        com.linkedin.versionset.VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);

    com.linkedin.entity.EnvelopedAspect envelopedVp = new com.linkedin.entity.EnvelopedAspect();
    envelopedVp.setValue(new com.linkedin.entity.Aspect(vp.data()));
    com.linkedin.entity.EnvelopedAspectMap aspectMap = new com.linkedin.entity.EnvelopedAspectMap();
    aspectMap.put(VERSION_PROPERTIES_ASPECT_NAME, envelopedVp);
    GlossaryTermInfo prevInfo =
        new GlossaryTermInfo()
            .setName("Revenue v2")
            .setDefinition("Old definition.")
            .setTermSource("INTERNAL");
    com.linkedin.entity.EnvelopedAspect envelopedInfo = new com.linkedin.entity.EnvelopedAspect();
    envelopedInfo.setValue(new com.linkedin.entity.Aspect(prevInfo.data()));
    aspectMap.put(GLOSSARY_TERM_INFO_ASPECT_NAME, envelopedInfo);
    com.linkedin.entity.EntityResponse entityResponse = new com.linkedin.entity.EntityResponse();
    entityResponse.setAspects(aspectMap);

    Mockito.when(
            mockClient.getV2(
                any(),
                eq(GLOSSARY_TERM_ENTITY_NAME),
                eq(UrnUtils.getUrn(TEST_PREV_TERM_URN)),
                any()))
        .thenReturn(entityResponse);

    IngestResult mockResult =
        IngestResult.builder().urn(Urn.createFromString(TEST_TERM_URN)).build();
    Mockito.when(
            mockVersioningService.linkLatestVersion(
                any(),
                eq(UrnUtils.getUrn(TEST_VERSION_SET_URN)),
                eq(UrnUtils.getUrn(TEST_TERM_URN)),
                any(VersionPropertiesInput.class)))
        .thenReturn(ImmutableList.of(mockResult));

    CreateGlossaryTermVersionResolver resolver =
        new CreateGlossaryTermVersionResolver(
            mockClient, mockService, mockVersioningService, mockFlags);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    CreateGlossaryTermVersionInput input = new CreateGlossaryTermVersionInput();
    input.setName("Revenue v3");
    input.setDescription("Updated definition.");
    input.setVersion("v3");
    input.setPreviousTermUrn(TEST_PREV_TERM_URN);

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();

    assertNotNull(result);
    assertEquals(result.getUrn(), TEST_VERSION_SET_URN);
  }

  @Test
  public void testGetFeatureFlagDisabled() {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = getMockEntityService();
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(false);

    CreateGlossaryTermVersionResolver resolver =
        new CreateGlossaryTermVersionResolver(
            mockClient, mockService, mockVersioningService, mockFlags);

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    CreateGlossaryTermVersionInput input = new CreateGlossaryTermVersionInput();
    input.setName("Revenue v3");
    input.setDescription("Updated definition.");
    input.setVersion("v3");
    input.setVersionSet(TEST_VERSION_SET_URN);

    // Feature flag check fires before context is ever accessed — no context stub needed
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);

    assertThrows(IllegalAccessError.class, () -> resolver.get(mockEnv));
  }

  @Test
  public void testGetInvalidVersionSetUrnType() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = getMockEntityService();
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);

    CreateGlossaryTermVersionResolver resolver =
        new CreateGlossaryTermVersionResolver(
            mockClient, mockService, mockVersioningService, mockFlags);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    CreateGlossaryTermVersionInput input = new CreateGlossaryTermVersionInput();
    input.setName("Revenue v3");
    input.setDescription("Updated definition.");
    input.setVersion("v3");
    // Wrong entity type — glossaryTerm URN instead of versionSet URN
    input.setVersionSet("urn:li:glossaryTerm:NotAVersionSet");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
  }

  @Test
  public void testGetPreviousTermMissing() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = getMockEntityService();
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);

    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);
    // Return null — the previous term does not exist
    Mockito.when(mockClient.getV2(any(), eq(GLOSSARY_TERM_ENTITY_NAME), any(), any()))
        .thenReturn(null);

    CreateGlossaryTermVersionResolver resolver =
        new CreateGlossaryTermVersionResolver(
            mockClient, mockService, mockVersioningService, mockFlags);

    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);

    CreateGlossaryTermVersionInput input = new CreateGlossaryTermVersionInput();
    input.setName("Revenue v3");
    input.setDescription("Updated definition.");
    input.setVersion("v3");
    input.setPreviousTermUrn("urn:li:glossaryTerm:NotVersioned");

    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(ExecutionException.class, () -> resolver.get(mockEnv).get());
  }

  private static final String TEST_PARENT_NODE_URN = "urn:li:glossaryNode:my_node";

  /**
   * Builds a previous-term response with info (under TEST_PARENT_NODE_URN) and, optionally, version
   * properties.
   */
  private static com.linkedin.entity.EntityResponse previousTermResponse(boolean versioned)
      throws Exception {
    com.linkedin.entity.EnvelopedAspectMap aspectMap = new com.linkedin.entity.EnvelopedAspectMap();
    GlossaryTermInfo info =
        new GlossaryTermInfo()
            .setName("Revenue")
            .setDefinition("Old definition.")
            .setTermSource("INTERNAL")
            .setParentNode(GlossaryNodeUrn.createFromString(TEST_PARENT_NODE_URN));
    aspectMap.put(
        GLOSSARY_TERM_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new com.linkedin.entity.Aspect(info.data())));
    if (versioned) {
      com.linkedin.common.VersionProperties vp = new com.linkedin.common.VersionProperties();
      vp.setVersionSet(UrnUtils.getUrn(TEST_VERSION_SET_URN));
      vp.setVersion(new com.linkedin.common.VersionTag().setVersionTag("v2"));
      vp.setSortId("AAAAAAAB");
      vp.setVersioningScheme(
          com.linkedin.versionset.VersioningScheme.ALPHANUMERIC_GENERATED_BY_DATAHUB);
      aspectMap.put(
          VERSION_PROPERTIES_ASPECT_NAME,
          new com.linkedin.entity.EnvelopedAspect()
              .setValue(new com.linkedin.entity.Aspect(vp.data())));
    }
    return new com.linkedin.entity.EntityResponse().setAspects(aspectMap);
  }

  @Test
  public void testUnversionedPreviousTermBecomesFirstVersion() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = getMockEntityService();
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);
    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);
    Mockito.when(mockClient.exists(any(), any())).thenReturn(false);
    Mockito.when(mockClient.ingestProposal(any(), any(), anyBoolean())).thenReturn(TEST_TERM_URN);
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(GLOSSARY_TERM_ENTITY_NAME),
                eq(UrnUtils.getUrn(TEST_PREV_TERM_URN)),
                any()))
        .thenReturn(previousTermResponse(false));
    Mockito.when(mockVersioningService.linkLatestVersion(any(), any(), any(), any()))
        .thenReturn(
            ImmutableList.of(
                IngestResult.builder().urn(Urn.createFromString(TEST_TERM_URN)).build()));

    CreateGlossaryTermVersionResolver resolver =
        new CreateGlossaryTermVersionResolver(
            mockClient, mockService, mockVersioningService, mockFlags);
    CreateGlossaryTermVersionInput input = new CreateGlossaryTermVersionInput();
    input.setName("Revenue");
    input.setVersion("v2");
    input.setPreviousTermUrn(TEST_PREV_TERM_URN);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    var result = resolver.get(mockEnv).get();

    ArgumentCaptor<Urn> setCaptor = ArgumentCaptor.forClass(Urn.class);
    ArgumentCaptor<Urn> termCaptor = ArgumentCaptor.forClass(Urn.class);
    ArgumentCaptor<VersionPropertiesInput> propsCaptor =
        ArgumentCaptor.forClass(VersionPropertiesInput.class);
    Mockito.verify(mockVersioningService, Mockito.times(2))
        .linkLatestVersion(any(), setCaptor.capture(), termCaptor.capture(), propsCaptor.capture());
    // The previous term is linked first, as "v1", into the same new version set.
    assertEquals(termCaptor.getAllValues().get(0), UrnUtils.getUrn(TEST_PREV_TERM_URN));
    assertEquals(propsCaptor.getAllValues().get(0).getVersion(), "v1");
    assertEquals(termCaptor.getAllValues().get(1), UrnUtils.getUrn(TEST_TERM_URN));
    assertEquals(propsCaptor.getAllValues().get(1).getVersion(), "v2");
    assertEquals(setCaptor.getAllValues().get(0), setCaptor.getAllValues().get(1));
    assertEquals(result.getUrn(), setCaptor.getAllValues().get(0).toString());
  }

  @Test
  public void testNewVersionUsesPreviousTermParent() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityService mockService = getMockEntityService();
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);
    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);
    Mockito.when(mockClient.exists(any(), any())).thenReturn(false);
    Mockito.when(mockClient.ingestProposal(any(), any(), anyBoolean())).thenReturn(TEST_TERM_URN);
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(GLOSSARY_TERM_ENTITY_NAME),
                eq(UrnUtils.getUrn(TEST_PREV_TERM_URN)),
                any()))
        .thenReturn(previousTermResponse(true));
    Mockito.when(mockVersioningService.linkLatestVersion(any(), any(), any(), any()))
        .thenReturn(
            ImmutableList.of(
                IngestResult.builder().urn(Urn.createFromString(TEST_TERM_URN)).build()));

    CreateGlossaryTermVersionResolver resolver =
        new CreateGlossaryTermVersionResolver(
            mockClient, mockService, mockVersioningService, mockFlags);
    CreateGlossaryTermVersionInput input = new CreateGlossaryTermVersionInput();
    input.setName("Revenue");
    input.setVersion("v3");
    input.setPreviousTermUrn(TEST_PREV_TERM_URN);
    QueryContext mockContext = getMockAllowContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    resolver.get(mockEnv).get();

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockClient, Mockito.atLeastOnce())
        .ingestProposal(any(), mcpCaptor.capture(), anyBoolean());
    MetadataChangeProposal infoMcp =
        mcpCaptor.getAllValues().stream()
            .filter(m -> GLOSSARY_TERM_INFO_ASPECT_NAME.equals(m.getAspectName()))
            .findFirst()
            .orElseThrow();
    GlossaryTermInfo written =
        GenericRecordUtils.deserializeAspect(
            infoMcp.getAspect().getValue(),
            infoMcp.getAspect().getContentType(),
            GlossaryTermInfo.class);
    assertEquals(written.getParentNode().toString(), TEST_PARENT_NODE_URN);
  }

  private static final String DOMAIN_URN = "urn:li:domain:my_domain";

  private static final StructuredProperties PREVIOUS_STRUCTURED_PROPERTIES =
      new StructuredProperties()
          .setProperties(
              new StructuredPropertyValueAssignmentArray(
                  new StructuredPropertyValueAssignment()
                      .setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:formula"))
                      .setValues(
                          new PrimitivePropertyValueArray(
                              PrimitivePropertyValue.create("gross - returns")))));

  /**
   * Mocks for a versioned previous term that has a domain, an owner, tags, structured properties
   * and a deprecation, and a DRAFT stage.
   */
  private static CreateGlossaryTermVersionResolver resolverWithPreviousMetadata(
      EntityClient mockClient,
      EntityVersioningService mockVersioningService,
      List<String> draftEntityTypes)
      throws Exception {
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);
    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);
    Mockito.when(mockClient.exists(any(), any())).thenReturn(false);
    Mockito.when(mockClient.ingestProposal(any(), any(), anyBoolean())).thenReturn(TEST_TERM_URN);

    com.linkedin.entity.EntityResponse previous = previousTermResponse(true);
    previous
        .getAspects()
        .put(
            DOMAINS_ASPECT_NAME,
            new com.linkedin.entity.EnvelopedAspect()
                .setValue(
                    new com.linkedin.entity.Aspect(
                        new Domains()
                            .setDomains(new UrnArray(List.of(UrnUtils.getUrn(DOMAIN_URN))))
                            .data())));
    previous
        .getAspects()
        .put(
            OWNERSHIP_ASPECT_NAME,
            new com.linkedin.entity.EnvelopedAspect()
                .setValue(
                    new com.linkedin.entity.Aspect(
                        new Ownership()
                            .setOwners(
                                new OwnerArray(
                                    List.of(
                                        new Owner()
                                            .setOwner(
                                                UrnUtils.getUrn("urn:li:corpGroup:publishers"))
                                            .setType(OwnershipType.BUSINESS_OWNER))))
                            .data())));
    previous
        .getAspects()
        .put(
            STRUCTURED_PROPERTIES_ASPECT_NAME,
            new com.linkedin.entity.EnvelopedAspect()
                .setValue(new com.linkedin.entity.Aspect(PREVIOUS_STRUCTURED_PROPERTIES.data())));
    previous
        .getAspects()
        .put(
            GLOBAL_TAGS_ASPECT_NAME,
            new com.linkedin.entity.EnvelopedAspect()
                .setValue(
                    new com.linkedin.entity.Aspect(
                        new GlobalTags()
                            .setTags(
                                new TagAssociationArray(
                                    new TagAssociation()
                                        .setTag(TagUrn.createFromString("urn:li:tag:kpi"))))
                            .data())));
    previous
        .getAspects()
        .put(
            DEPRECATION_ASPECT_NAME,
            new com.linkedin.entity.EnvelopedAspect()
                .setValue(
                    new com.linkedin.entity.Aspect(
                        new Deprecation()
                            .setDeprecated(true)
                            .setNote("")
                            .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                            .data())));
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(GLOSSARY_TERM_ENTITY_NAME),
                eq(UrnUtils.getUrn(TEST_PREV_TERM_URN)),
                any()))
        .thenReturn(previous);

    stubDraftStage(
        mockClient,
        new LifecycleStageTypeInfo()
            .setName("Draft")
            .setEntityTypes(new StringArray(draftEntityTypes))
            .setSettings(new LifecycleStageSettings()));

    Mockito.when(mockVersioningService.linkLatestVersion(any(), any(), any(), any()))
        .thenReturn(
            ImmutableList.of(
                IngestResult.builder().urn(Urn.createFromString(TEST_TERM_URN)).build()));
    return new CreateGlossaryTermVersionResolver(
        mockClient, getMockEntityService(), mockVersioningService, mockFlags);
  }

  private static void stubDraftStage(EntityClient mockClient, LifecycleStageTypeInfo draftInfo)
      throws Exception {
    com.linkedin.entity.EnvelopedAspectMap draftAspects =
        new com.linkedin.entity.EnvelopedAspectMap();
    draftAspects.put(
        LifecycleStages.STAGE_INFO_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(new com.linkedin.entity.Aspect(draftInfo.data())));
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(LifecycleStages.STAGE_ENTITY_NAME),
                eq(UrnUtils.getUrn(DRAFT_LIFECYCLE_STAGE_URN)),
                any()))
        .thenReturn(new com.linkedin.entity.EntityResponse().setAspects(draftAspects));
  }

  private static DataFetchingEnvironment envFor(
      CreateGlossaryTermVersionInput input, QueryContext context) {
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(context);
    return mockEnv;
  }

  private static DataFetchingEnvironment envFor(CreateGlossaryTermVersionInput input) {
    QueryContext context = getMockAllowContext();
    return envFor(input, context);
  }

  private static CreateGlossaryTermVersionInput newVersionInput() {
    CreateGlossaryTermVersionInput input = new CreateGlossaryTermVersionInput();
    input.setName("Revenue");
    input.setVersion("v3");
    input.setPreviousTermUrn(TEST_PREV_TERM_URN);
    return input;
  }

  private static List<MetadataChangeProposal> ingested(EntityClient mockClient) throws Exception {
    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    Mockito.verify(mockClient, Mockito.atLeastOnce())
        .ingestProposal(any(), captor.capture(), anyBoolean());
    return captor.getAllValues();
  }

  @Test
  public void testCopiesPreviousMetadataToNewTerm() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));

    resolver.get(envFor(newVersionInput())).get();

    List<MetadataChangeProposal> copied =
        ingested(mockClient).stream()
            .filter(
                m ->
                    Set.of(
                            DOMAINS_ASPECT_NAME,
                            OWNERSHIP_ASPECT_NAME,
                            GLOBAL_TAGS_ASPECT_NAME,
                            STRUCTURED_PROPERTIES_ASPECT_NAME)
                        .contains(m.getAspectName()))
            .collect(Collectors.toList());
    assertEquals(copied.size(), 4);
    // A deprecation belongs to the old version; the new one starts undeprecated.
    assertTrue(
        ingested(mockClient).stream()
            .noneMatch(m -> DEPRECATION_ASPECT_NAME.equals(m.getAspectName())));
    // Every copied aspect targets the new term, not the previous one.
    assertTrue(
        copied.stream()
            .allMatch(
                m ->
                    m.getEntityUrn() != null && TEST_TERM_URN.equals(m.getEntityUrn().toString())));
    MetadataChangeProposal propertiesMcp =
        copied.stream()
            .filter(m -> STRUCTURED_PROPERTIES_ASPECT_NAME.equals(m.getAspectName()))
            .findFirst()
            .orElseThrow();
    assertEquals(
        GenericRecordUtils.deserializeAspect(
            propertiesMcp.getAspect().getValue(),
            propertiesMcp.getAspect().getContentType(),
            StructuredProperties.class),
        PREVIOUS_STRUCTURED_PROPERTIES);
  }

  @Test
  public void testCopyMetadataFalseCopiesNothing() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));
    CreateGlossaryTermVersionInput input = newVersionInput();
    input.setCopyMetadata(false);

    resolver.get(envFor(input)).get();

    Set<String> aspectNames =
        ingested(mockClient).stream()
            .map(MetadataChangeProposal::getAspectName)
            .collect(Collectors.toSet());
    assertFalse(aspectNames.contains(DOMAINS_ASPECT_NAME));
    assertFalse(aspectNames.contains(OWNERSHIP_ASPECT_NAME));
  }

  @Test
  public void testStartsInDraftBeforeLinking() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));

    resolver.get(envFor(newVersionInput())).get();

    MetadataChangeProposal statusMcp =
        ingested(mockClient).stream()
            .filter(m -> STATUS_ASPECT_NAME.equals(m.getAspectName()))
            .findFirst()
            .orElseThrow();
    Status status =
        GenericRecordUtils.deserializeAspect(
            statusMcp.getAspect().getValue(), statusMcp.getAspect().getContentType(), Status.class);
    assertEquals(status.getLifecycleStage().toString(), DRAFT_LIFECYCLE_STAGE_URN);

    // The draft stage must be set before linking: VersionPropertiesSideEffect then keeps the
    // previous version latest. Drafting after linking relies on a search for siblings that does
    // not yet see the previous version, leaving no version latest.
    InOrder order = Mockito.inOrder(mockVersioningService, mockClient);
    order
        .verify(mockClient)
        .ingestProposal(
            any(),
            Mockito.argThat(m -> m != null && STATUS_ASPECT_NAME.equals(m.getAspectName())),
            anyBoolean());
    order
        .verify(mockVersioningService)
        .linkLatestVersion(any(), any(), eq(UrnUtils.getUrn(TEST_TERM_URN)), any());
  }

  @Test
  public void testSkipsDraftWhenStageDoesNotApply() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("document"));

    resolver.get(envFor(newVersionInput())).get();

    assertTrue(
        ingested(mockClient).stream().noneMatch(m -> STATUS_ASPECT_NAME.equals(m.getAspectName())));
  }

  @Test
  public void testVersionSetOfAnotherEntityTypeIsRejectedBeforeAnyWrite() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);
    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);
    Mockito.when(mockClient.exists(any(), any())).thenReturn(false);
    CreateGlossaryTermVersionResolver resolver =
        new CreateGlossaryTermVersionResolver(
            mockClient, getMockEntityService(), mockVersioningService, mockFlags);
    CreateGlossaryTermVersionInput input = new CreateGlossaryTermVersionInput();
    input.setName("Revenue v3");
    input.setVersion("v3");
    input.setVersionSet("urn:li:versionSet:(kpi-revenue,dataset)");

    ExecutionException e =
        expectThrows(
            ExecutionException.class,
            () -> resolver.get(envFor(input, getMockAllowContext())).get());

    assertTrue(e.getCause() instanceof IllegalArgumentException);
    Mockito.verify(mockClient, Mockito.never()).ingestProposal(any(), any(), anyBoolean());
    Mockito.verify(mockVersioningService, Mockito.never())
        .linkLatestVersion(any(), any(), any(), any());
  }

  @Test
  public void testSkipsDraftWhenStageIsDisabled() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of());

    resolver.get(envFor(newVersionInput())).get();

    assertTrue(
        ingested(mockClient).stream().noneMatch(m -> STATUS_ASPECT_NAME.equals(m.getAspectName())));
  }

  @Test
  public void testExistingIdIsRejectedBeforeLinkingPreviousTerm() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    FeatureFlags mockFlags = Mockito.mock(FeatureFlags.class);
    Mockito.when(mockFlags.isEntityVersioning()).thenReturn(true);
    Mockito.when(mockClient.exists(any(), any())).thenReturn(true);
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(GLOSSARY_TERM_ENTITY_NAME),
                eq(UrnUtils.getUrn(TEST_PREV_TERM_URN)),
                any()))
        .thenReturn(previousTermResponse(false));
    CreateGlossaryTermVersionResolver resolver =
        new CreateGlossaryTermVersionResolver(
            mockClient, getMockEntityService(), mockVersioningService, mockFlags);
    CreateGlossaryTermVersionInput input = newVersionInput();
    input.setId("taken");

    assertThrows(ExecutionException.class, () -> resolver.get(envFor(input)).get());

    Mockito.verify(mockVersioningService, Mockito.never())
        .linkLatestVersion(any(), any(), any(), any());
  }

  /** Grants glossary-children management on TEST_PARENT_NODE_URN only; no platform-wide grant. */
  private static QueryContext contextManagingOnlyTestParent() {
    return contextManagingOnly(TEST_PARENT_NODE_URN);
  }

  private static QueryContext contextManagingOnly(String nodeUrn) {
    return getMockAllowContextForResource(
        "urn:li:corpuser:test",
        PoliciesConfig.MANAGE_GLOSSARY_CHILDREN_PRIVILEGE.getType(),
        UrnUtils.getUrn(nodeUrn));
  }

  @Test
  public void testAuthorizedAgainstPreviousTermsParent() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));
    QueryContext context = contextManagingOnlyTestParent();

    // No parentNode in the input: the previous term's parent is the one that gets checked.
    resolver.get(envFor(newVersionInput(), context)).get();

    Mockito.verify(mockVersioningService).linkLatestVersion(any(), any(), any(), any());
    Mockito.verify(mockClient, Mockito.atLeastOnce()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testRightsOnlyOnExplicitParentAreRefusedWhenPreviousTermIsElsewhere()
      throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));
    QueryContext context = contextManagingOnly("urn:li:glossaryNode:other_node");
    CreateGlossaryTermVersionInput input = newVersionInput();
    input.setParentNode("urn:li:glossaryNode:other_node");

    ExecutionException e =
        expectThrows(ExecutionException.class, () -> resolver.get(envFor(input, context)).get());

    assertTrue(e.getCause() instanceof AuthorizationException);
    Mockito.verify(mockClient, Mockito.never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testRightsOnlyOnPreviousTermsNodeAreRefusedWithExplicitParent() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));
    QueryContext context = contextManagingOnlyTestParent();
    CreateGlossaryTermVersionInput input = newVersionInput();
    input.setParentNode("urn:li:glossaryNode:other_node");

    ExecutionException e =
        expectThrows(ExecutionException.class, () -> resolver.get(envFor(input, context)).get());

    assertTrue(e.getCause() instanceof AuthorizationException);
    Mockito.verify(mockClient, Mockito.never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testExplicitParentSucceedsWithRightsOnBothNodes() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));
    QueryContext context = getMockAllowContext();
    CreateGlossaryTermVersionInput input = newVersionInput();
    input.setParentNode("urn:li:glossaryNode:other_node");

    resolver.get(envFor(input, context)).get();

    Mockito.verify(mockVersioningService).linkLatestVersion(any(), any(), any(), any());
  }

  /** Stubs the version set's latest pointer and the version with the highest sortId. */
  private static void stubVersionSet(EntityClient mockClient, String latest, String highest)
      throws Exception {
    com.linkedin.entity.EnvelopedAspectMap aspects = new com.linkedin.entity.EnvelopedAspectMap();
    aspects.put(
        VERSION_SET_PROPERTIES_ASPECT_NAME,
        new com.linkedin.entity.EnvelopedAspect()
            .setValue(
                new com.linkedin.entity.Aspect(
                    new VersionSetProperties()
                        .setLatest(UrnUtils.getUrn(latest))
                        .setVersioningScheme(
                            com.linkedin.versionset.VersioningScheme
                                .ALPHANUMERIC_GENERATED_BY_DATAHUB)
                        .data())));
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(VERSION_SET_ENTITY_NAME),
                eq(UrnUtils.getUrn(TEST_VERSION_SET_URN)),
                any()))
        .thenReturn(new com.linkedin.entity.EntityResponse().setAspects(aspects));
    Mockito.when(
            mockClient.search(
                any(), eq(GLOSSARY_TERM_ENTITY_NAME), any(), any(), any(), eq(0), eq(1)))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(new SearchEntity().setEntity(UrnUtils.getUrn(highest)))));
  }

  @Test
  public void testRejectsWhenAnUnpublishedNewerVersionExists() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));
    stubVersionSet(mockClient, TEST_PREV_TERM_URN, "urn:li:glossaryTerm:Revenue_draft");

    ExecutionException e =
        expectThrows(ExecutionException.class, () -> resolver.get(envFor(newVersionInput())).get());

    assertTrue(e.getCause() instanceof IllegalArgumentException);
    Mockito.verify(mockClient, Mockito.never()).ingestProposal(any(), any(), anyBoolean());
    Mockito.verify(mockVersioningService, Mockito.never())
        .linkLatestVersion(any(), any(), any(), any());
  }

  @Test
  public void testAllowsNewVersionWhenLatestIsNewest() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));
    stubVersionSet(mockClient, TEST_PREV_TERM_URN, TEST_PREV_TERM_URN);

    resolver.get(envFor(newVersionInput())).get();

    Mockito.verify(mockVersioningService)
        .linkLatestVersion(any(), any(), eq(UrnUtils.getUrn(TEST_TERM_URN)), any());
  }

  @Test
  public void testRejectsBeforeWritingWhenDraftCannotBeEntered() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));
    stubDraftStage(
        mockClient,
        new LifecycleStageTypeInfo()
            .setName("Draft")
            .setEntityTypes(new StringArray(List.of("glossaryTerm")))
            .setSettings(new LifecycleStageSettings())
            .setTransitionPolicy(
                new LifecycleStageTransitionPolicy()
                    .setAllowedPreviousStages(
                        new UrnArray(
                            List.of(UrnUtils.getUrn("urn:li:lifecycleStageType:REVIEW"))))));

    ExecutionException e =
        expectThrows(ExecutionException.class, () -> resolver.get(envFor(newVersionInput())).get());

    assertTrue(e.getCause() instanceof IllegalArgumentException);
    Mockito.verify(mockClient, Mockito.never()).ingestProposal(any(), any(), anyBoolean());
    Mockito.verify(mockVersioningService, Mockito.never())
        .linkLatestVersion(any(), any(), any(), any());
  }

  /** Points the previous term at the glossary root. */
  private static void stubPreviousTermAtRoot(EntityClient mockClient) throws Exception {
    com.linkedin.entity.EntityResponse previous = previousTermResponse(true);
    GlossaryTermInfo info =
        new GlossaryTermInfo()
            .setName("Revenue")
            .setDefinition("Old definition.")
            .setTermSource("INTERNAL");
    previous
        .getAspects()
        .put(
            GLOSSARY_TERM_INFO_ASPECT_NAME,
            new com.linkedin.entity.EnvelopedAspect()
                .setValue(new com.linkedin.entity.Aspect(info.data())));
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(GLOSSARY_TERM_ENTITY_NAME),
                eq(UrnUtils.getUrn(TEST_PREV_TERM_URN)),
                any()))
        .thenReturn(previous);
  }

  @Test
  public void testRootPreviousTermWithExplicitParentNeedsPlatformRights() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    EntityVersioningService mockVersioningService = Mockito.mock(EntityVersioningService.class);
    CreateGlossaryTermVersionResolver resolver =
        resolverWithPreviousMetadata(mockClient, mockVersioningService, List.of("glossaryTerm"));
    stubPreviousTermAtRoot(mockClient);
    CreateGlossaryTermVersionInput input = newVersionInput();
    input.setParentNode(TEST_PARENT_NODE_URN);

    // A steward of the target node alone cannot touch a term at the glossary root.
    ExecutionException e =
        expectThrows(
            ExecutionException.class,
            () -> resolver.get(envFor(input, contextManagingOnlyTestParent())).get());
    assertTrue(e.getCause() instanceof AuthorizationException);
    Mockito.verify(mockClient, Mockito.never()).ingestProposal(any(), any(), anyBoolean());

    resolver.get(envFor(input, getMockAllowContext())).get();
    Mockito.verify(mockVersioningService).linkLatestVersion(any(), any(), any(), any());
  }
}
