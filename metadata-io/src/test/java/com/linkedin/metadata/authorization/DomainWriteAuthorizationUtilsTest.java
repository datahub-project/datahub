package com.linkedin.metadata.authorization;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.authorization.EntityFieldType;
import com.datahub.authorization.FieldResolver;
import com.datahub.authorization.ResolvedEntitySpec;
import com.datahub.context.OperationFingerprint;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.patch.template.AspectTemplateEngine;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.metadata.aspect.patch.template.common.DomainsTemplate;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

public class DomainWriteAuthorizationUtilsTest {

  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");
  private static final Urn CHART_URN = UrnUtils.getUrn("urn:li:chart:(looker,chart.1)");
  private static final Urn DOMAIN_X = UrnUtils.getUrn("urn:li:domain:engineering");
  private static final Urn DOMAIN_Y = UrnUtils.getUrn("urn:li:domain:marketing");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final AuditStamp AUDIT_STAMP =
      new AuditStamp().setActor(UrnUtils.getUrn("urn:li:corpuser:datahub")).setTime(0L);

  @Test
  public void testShouldUseProposedDomains_newEntityWithDomains() {
    assertTrue(DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(false, false, true));
  }

  @Test
  public void testShouldUseProposedDomains_newEntityWithoutDomains() {
    assertFalse(
        DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(false, false, false));
  }

  @Test
  public void testShouldUseProposedDomains_existingEntityMissingDomains() {
    assertTrue(DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(true, false, true));
  }

  @Test
  public void testShouldUseProposedDomains_existingEntityWithDomains() {
    assertFalse(DomainWriteAuthorizationUtils.shouldUseProposedDomainsForMatch(true, true, true));
  }

  @Test
  public void testResolveApiOperation_upsertNewEntityUsesCreate() {
    assertEquals(
        DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.UPSERT, false),
        ApiOperation.CREATE);
  }

  @Test
  public void testResolveApiOperation_upsertExistingUsesUpdate() {
    assertEquals(
        DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.UPSERT, true),
        ApiOperation.UPDATE);
  }

  @Test
  public void testResolveApiOperation_createEntityOnExistingUsesUpdate() {
    assertEquals(
        DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.CREATE_ENTITY, true),
        ApiOperation.UPDATE);
  }

  @Test
  public void testResolveApiOperation_aspectCreateAlwaysUpdate() {
    assertEquals(
        DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.CREATE, false),
        ApiOperation.UPDATE);
  }

  @Test
  public void testResolveApiOperation_patchAlwaysUpdate() {
    assertEquals(
        DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.PATCH, false),
        ApiOperation.UPDATE);
    assertEquals(
        DomainWriteAuthorizationUtils.resolveApiOperation(ChangeType.PATCH, true),
        ApiOperation.UPDATE);
  }

  @Test
  public void testHasDomainMembership() {
    assertFalse(DomainWriteAuthorizationUtils.hasDomainMembership(null));
    assertFalse(DomainWriteAuthorizationUtils.hasDomainMembership(new Domains()));
    assertTrue(
        DomainWriteAuthorizationUtils.hasDomainMembership(
            new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)))));
  }

  @Test
  public void testIsAuthorizedDomainsEdit_nullAfterWithBeforeDenies() {
    AuthorizationSession session = mock(AuthorizationSession.class);
    Domains before = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    assertFalse(
        DomainWriteAuthorizationUtils.isAuthorizedDomainsEdit(session, DATASET_URN, before, null));
  }

  @Test
  public void testExtractProposedDomainsByUrn() {
    Domains domains = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    AspectSpec domainsSpec = mock(AspectSpec.class);
    when(domainsSpec.getName()).thenReturn("domains");
    EntitySpec entitySpec = mock(EntitySpec.class);
    when(entitySpec.getName()).thenReturn("dataset");

    TestMCP item =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(domainsSpec)
            .recordTemplate(domains)
            .changeType(ChangeType.CREATE_ENTITY)
            .build();

    Map<Urn, Domains> proposed =
        DomainWriteAuthorizationUtils.extractProposedDomainsByUrn(List.of(item));
    assertEquals(proposed.size(), 1);
    assertEquals(
        EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(proposed.get(DATASET_URN)),
        Set.of(DOMAIN_X));
  }

  @Test
  public void testExtractProposedDomainsFromItem_perItem() {
    Domains domainsA = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    Domains domainsB = new Domains().setDomains(new UrnArray(List.of(DOMAIN_Y)));
    AspectSpec domainsSpec = mock(AspectSpec.class);
    when(domainsSpec.getName()).thenReturn("domains");
    EntitySpec entitySpec = mock(EntitySpec.class);
    when(entitySpec.getName()).thenReturn("dataset");

    TestMCP first =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(domainsSpec)
            .recordTemplate(domainsB)
            .changeType(ChangeType.UPSERT)
            .build();
    TestMCP second =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(domainsSpec)
            .recordTemplate(domainsA)
            .changeType(ChangeType.UPSERT)
            .build();

    assertEquals(
        EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(
            DomainWriteAuthorizationUtils.extractProposedDomainsFromItem(first)),
        Set.of(DOMAIN_Y));
    assertEquals(
        EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(
            DomainWriteAuthorizationUtils.extractProposedDomainsFromItem(second)),
        Set.of(DOMAIN_X));
  }

  @Test
  public void testResolveDomainsAspectExists_groupsByEntityType() {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    Domains chartDomains = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    when(aspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(DATASET_URN)), eq(Set.of("domains"))))
        .thenReturn(Map.of(DATASET_URN, Map.of()));
    when(aspectRetriever.getLatestAspectObjects(
            any(), eq(Set.of(CHART_URN)), eq(Set.of("domains"))))
        .thenReturn(Map.of(CHART_URN, Map.of("domains", new Aspect(chartDomains.data()))));

    Map<Urn, Boolean> result =
        DomainWriteAuthorizationUtils.resolveDomainsAspectExists(
            OperationFingerprint.EMPTY, aspectRetriever, List.of(DATASET_URN, CHART_URN));

    assertFalse(result.get(DATASET_URN));
    assertTrue(result.get(CHART_URN));
    verify(aspectRetriever, times(1))
        .getLatestAspectObjects(any(), eq(Set.of(DATASET_URN)), eq(Set.of("domains")));
    verify(aspectRetriever, times(1))
        .getLatestAspectObjects(any(), eq(Set.of(CHART_URN)), eq(Set.of("domains")));
  }

  @Test
  public void testExtractProposedDomainsFromItem_skipsPatchWithoutDeserializing() {
    BatchItem patchItem = mock(BatchItem.class);
    when(patchItem.getAspectName()).thenReturn("domains");
    when(patchItem.getChangeType()).thenReturn(ChangeType.PATCH);
    when(patchItem.getUrn()).thenReturn(DATASET_URN);
    when(patchItem.getAspect(Domains.class))
        .thenThrow(
            new IllegalArgumentException(
                "application/json-patch+json content type is not supported"));
    when(patchItem.getRecordTemplate())
        .thenThrow(
            new IllegalArgumentException(
                "application/json-patch+json content type is not supported"));

    assertNull(DomainWriteAuthorizationUtils.extractProposedDomainsFromItem(patchItem));
    verify(patchItem, never()).getAspect(Domains.class);
    verify(patchItem, never()).getRecordTemplate();
  }

  @Test
  public void testResolveProposedDomainsForItem_patchAppliesAgainstEmpty() throws Exception {
    EntityRegistry registry = domainsPatchEntityRegistry();
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(registry);

    MCPItem proposed = domainsAddPatchItem(registry, DOMAIN_X);

    Domains resolved =
        DomainWriteAuthorizationUtils.resolveProposedDomainsForItem(
            proposed, aspectRetriever, null);
    assertEquals(
        EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(resolved), Set.of(DOMAIN_X));
  }

  @Test
  public void testResolveProposedDomainsForItem_patchFailureReturnsNull() {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(SnapshotEntityRegistry.getInstance());

    MCPItem badPatch = mock(MCPItem.class);
    when(badPatch.getAspectName()).thenReturn("domains");
    when(badPatch.getChangeType()).thenReturn(ChangeType.PATCH);
    when(badPatch.getUrn()).thenReturn(DATASET_URN);
    when(badPatch.getMetadataChangeProposal())
        .thenThrow(new IllegalStateException("unresolvable patch"));

    assertNull(
        DomainWriteAuthorizationUtils.resolveProposedDomainsForItem(
            badPatch, aspectRetriever, null));
  }

  @Test
  public void testResolveProposedDomainsForItem_skipsNonDomainsPatch() {
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    MCPItem tagsPatch = mock(MCPItem.class);
    when(tagsPatch.getAspectName()).thenReturn("globalTags");
    when(tagsPatch.getChangeType()).thenReturn(ChangeType.PATCH);
    when(tagsPatch.getUrn()).thenReturn(DATASET_URN);

    assertNull(
        DomainWriteAuthorizationUtils.resolveProposedDomainsForItem(
            tagsPatch, aspectRetriever, null));
    verify(tagsPatch, never()).getMetadataChangeProposal();
  }

  @Test
  public void testResolveProposedDomainsForItem_patchAppliesOverPriorInBatchDomains()
      throws Exception {
    EntityRegistry registry = domainsPatchEntityRegistry();
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(registry);

    Domains prior = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    MCPItem patchItem = domainsAddPatchItem(registry, DOMAIN_Y);

    Domains resolved =
        DomainWriteAuthorizationUtils.resolveProposedDomainsForItem(
            patchItem, aspectRetriever, new Aspect(prior.data()));
    assertEquals(
        EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(resolved),
        Set.of(DOMAIN_X, DOMAIN_Y));
  }

  @Test
  public void testAccumulateProposedDomainsFromBatchItems_upsertThenPatchChains() throws Exception {
    EntityRegistry registry = domainsPatchEntityRegistry();
    AspectRetriever aspectRetriever = mock(AspectRetriever.class);
    when(aspectRetriever.getEntityRegistry()).thenReturn(registry);

    AspectSpec domainsSpec = mock(AspectSpec.class);
    when(domainsSpec.getName()).thenReturn("domains");
    EntitySpec entitySpec = mock(EntitySpec.class);
    when(entitySpec.getName()).thenReturn("dataset");
    Domains upsertDomains = new Domains().setDomains(new UrnArray(List.of(DOMAIN_X)));
    TestMCP upsert =
        TestMCP.builder()
            .urn(DATASET_URN)
            .entitySpec(entitySpec)
            .aspectSpec(domainsSpec)
            .recordTemplate(upsertDomains)
            .changeType(ChangeType.UPSERT)
            .build();
    MCPItem patchItem = domainsAddPatchItem(registry, DOMAIN_Y);

    Map<Urn, Domains> proposed =
        DomainWriteAuthorizationUtils.accumulateProposedDomainsFromBatchItems(
            aspectRetriever, List.of(upsert, patchItem));

    assertEquals(
        EntityAspectAuthorizationUtils.resolveUniqueDomainUrns(proposed.get(DATASET_URN)),
        Set.of(DOMAIN_X, DOMAIN_Y));
  }

  @Test
  public void testSeedProposedDomainResourceSpec_mergesAndOverwritesDomain() {
    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    com.datahub.authorization.EntitySpec resourceSpec =
        new com.datahub.authorization.EntitySpec(
            DATASET_URN.getEntityType(), DATASET_URN.toString());

    Map<EntityFieldType, FieldResolver> existingResolvers = new EnumMap<>(EntityFieldType.class);
    existingResolvers.put(
        EntityFieldType.OWNER,
        FieldResolver.getResolverFromValues(Set.of("urn:li:corpuser:alice")));
    existingResolvers.put(
        EntityFieldType.DOMAIN, FieldResolver.getResolverFromValues(Set.of(DOMAIN_X.toString())));
    opContext
        .getAuthorizationContext()
        .getSessionResourceSpecCache()
        .put(resourceSpec, new ResolvedEntitySpec(resourceSpec, existingResolvers));

    DomainWriteAuthorizationUtils.seedProposedDomainResourceSpec(
        opContext, resourceSpec, Set.of(DOMAIN_Y.toString()));

    ResolvedEntitySpec merged =
        opContext.getAuthorizationContext().getSessionResourceSpecCache().get(resourceSpec);
    assertEquals(merged.getFieldValues(EntityFieldType.OWNER), Set.of("urn:li:corpuser:alice"));
    assertEquals(merged.getFieldValues(EntityFieldType.DOMAIN), Set.of(DOMAIN_Y.toString()));
  }

  @Nonnull
  private static EntityRegistry domainsPatchEntityRegistry() {
    return new TestEntityRegistry() {
      @Nonnull
      @Override
      public AspectTemplateEngine getAspectTemplateEngine() {
        Map<String, Template<? extends RecordTemplate>> aspectTemplateMap = new HashMap<>();
        aspectTemplateMap.put("domains", new DomainsTemplate());
        return new AspectTemplateEngine(aspectTemplateMap);
      }
    };
  }

  @Nonnull
  private static MCPItem domainsAddPatchItem(@Nonnull EntityRegistry registry, @Nonnull Urn domain)
      throws Exception {
    String encoded = domain.toString().replace("~", "~0").replace("/", "~1");
    String patchJson =
        "[{\"op\":\"add\",\"path\":\"/domains/" + encoded + "\",\"value\":\"" + domain + "\"}]";
    MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(DATASET_URN);
    mcp.setEntityType(DATASET_URN.getEntityType());
    mcp.setAspectName("domains");
    mcp.setChangeType(ChangeType.PATCH);
    mcp.setAspect(GenericRecordUtils.serializePatch(OBJECT_MAPPER.readTree(patchJson)));
    return ProposedItem.builder().build(mcp, AUDIT_STAMP, registry);
  }
}
