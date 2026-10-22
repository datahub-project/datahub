package com.linkedin.metadata.domains.sideeffects;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_KEY_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.key.DomainKey;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCL;
import io.datahubproject.metadata.context.RetrieverContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DomainReferenceDetachSideEffectTest {

  private static final EntityRegistry TEST_REGISTRY = new TestEntityRegistry();
  private static final Urn DOMAIN_URN = UrnUtils.getUrn("urn:li:domain:marketing");
  private static final Urn PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:campaign_metrics");
  private static final Urn DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");

  private static final AspectPluginConfig CONFIG =
      AspectPluginConfig.builder()
          .className(DomainReferenceDetachSideEffect.class.getName())
          .enabled(true)
          .supportedOperations(List.of("DELETE"))
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName(DOMAIN_ENTITY_NAME)
                      .aspectName(DOMAIN_KEY_ASPECT_NAME)
                      .build()))
          .build();

  private CachingAspectRetriever mockAspectRetriever;
  private SearchRetriever mockSearchRetriever;
  private RetrieverContext retrieverContext;

  @BeforeMethod
  public void setup() {
    mockAspectRetriever = mock(CachingAspectRetriever.class);
    mockSearchRetriever = mock(SearchRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(TEST_REGISTRY);
    retrieverContext =
        RetrieverContext.builder()
            .searchRetriever(mockSearchRetriever)
            .cachingAspectRetriever(mockAspectRetriever)
            .graphRetriever(mock(GraphRetriever.class))
            .build();
  }

  private MCLItem domainKeyMcl(ChangeType changeType) {
    MetadataChangeLog mcl = new MetadataChangeLog();
    mcl.setEntityUrn(DOMAIN_URN);
    mcl.setEntityType(DOMAIN_ENTITY_NAME);
    mcl.setAspectName(DOMAIN_KEY_ASPECT_NAME);
    mcl.setChangeType(changeType);
    mcl.setSystemMetadata(new SystemMetadata());
    return TestMCL.builder()
        .changeType(changeType)
        .urn(DOMAIN_URN)
        .entitySpec(TEST_REGISTRY.getEntitySpec(DOMAIN_ENTITY_NAME))
        .aspectSpec(
            TEST_REGISTRY.getEntitySpec(DOMAIN_ENTITY_NAME).getAspectSpec(DOMAIN_KEY_ASPECT_NAME))
        .recordTemplate(new DomainKey().setId("marketing"))
        .metadataChangeLog(mcl)
        .auditStamp(AuditStampUtils.createDefaultAuditStamp())
        .build();
  }

  private List<MCPItem> run(MCLItem item) {
    DomainReferenceDetachSideEffect test = new DomainReferenceDetachSideEffect();
    test.setConfig(CONFIG);
    return test.postMCPSideEffect(OperationFingerprint.EMPTY, List.of(item), retrieverContext)
        .toList();
  }

  private static Domains domainsAspect(Urn domainUrn) {
    return new Domains().setDomains(new UrnArray(domainUrn));
  }

  @Test
  public void testDomainKeyDeleteEmitsRemovePatchWhenPersistedAspectStillReferences() {
    ScrollResult scroll = new ScrollResult();
    SearchEntity hit = new SearchEntity();
    hit.setEntity(PRODUCT_URN);
    scroll.setEntities(new SearchEntityArray(List.of(hit)));
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any())).thenReturn(scroll);

    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(
            Map.of(
                PRODUCT_URN,
                Map.of(DOMAINS_ASPECT_NAME, new Aspect(domainsAspect(DOMAIN_URN).data()))));

    List<MCPItem> output = run(domainKeyMcl(ChangeType.DELETE));

    assertEquals(output.size(), 1);
    assertRemoveDomainPatch(output.get(0), PRODUCT_URN);
  }

  @Test
  public void testMixedEntityTypeScrollPageEmitsPatchForEachType() {
    ScrollResult scroll = new ScrollResult();
    SearchEntity productHit = new SearchEntity();
    productHit.setEntity(PRODUCT_URN);
    SearchEntity datasetHit = new SearchEntity();
    datasetHit.setEntity(DATASET_URN);
    scroll.setEntities(new SearchEntityArray(List.of(productHit, datasetHit)));
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any())).thenReturn(scroll);
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              Set<Urn> urns = invocation.getArgument(1);
              Map<Urn, Map<String, Aspect>> result = new HashMap<>();
              for (Urn urn : urns) {
                result.put(
                    urn, Map.of(DOMAINS_ASPECT_NAME, new Aspect(domainsAspect(DOMAIN_URN).data())));
              }
              return result;
            });

    List<MCPItem> output = run(domainKeyMcl(ChangeType.DELETE));

    assertEquals(output.size(), 2);
    assertEquals(
        output.stream().map(MCPItem::getUrn).collect(Collectors.toSet()),
        Set.of(PRODUCT_URN, DATASET_URN));
    output.forEach(item -> assertRemoveDomainPatch(item, item.getUrn()));
  }

  @Test
  public void testScrollRetriesThenSucceeds() {
    ScrollResult scroll = new ScrollResult();
    SearchEntity hit = new SearchEntity();
    hit.setEntity(PRODUCT_URN);
    scroll.setEntities(new SearchEntityArray(List.of(hit)));
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("timeout"))
        .thenReturn(scroll);
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(
            Map.of(
                PRODUCT_URN,
                Map.of(DOMAINS_ASPECT_NAME, new Aspect(domainsAspect(DOMAIN_URN).data()))));

    List<MCPItem> output = run(domainKeyMcl(ChangeType.DELETE));

    assertEquals(output.size(), 1);
    assertRemoveDomainPatch(output.get(0), PRODUCT_URN);
    verify(mockSearchRetriever, times(2)).scroll(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testScrollExhaustionFailsSoft() {
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any()))
        .thenThrow(new RuntimeException("timeout"));

    List<MCPItem> output = run(domainKeyMcl(ChangeType.DELETE));

    assertTrue(output.isEmpty());
    verify(mockSearchRetriever, times(DomainReferenceDetachSideEffect.MAX_PAGE_ATTEMPTS))
        .scroll(any(), any(), any(), any(), any(), any());
  }

  @Test
  public void testAspectReadRetriesThenSucceeds() {
    ScrollResult scroll = new ScrollResult();
    SearchEntity hit = new SearchEntity();
    hit.setEntity(PRODUCT_URN);
    scroll.setEntities(new SearchEntityArray(List.of(hit)));
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any())).thenReturn(scroll);
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenThrow(new RuntimeException("timeout"))
        .thenReturn(
            Map.of(
                PRODUCT_URN,
                Map.of(DOMAINS_ASPECT_NAME, new Aspect(domainsAspect(DOMAIN_URN).data()))));

    List<MCPItem> output = run(domainKeyMcl(ChangeType.DELETE));

    assertEquals(output.size(), 1);
    assertRemoveDomainPatch(output.get(0), PRODUCT_URN);
    verify(mockAspectRetriever, times(2)).getLatestAspectObjects(any(), any(), any());
  }

  @Test
  public void testAspectReadExhaustionSkipsPage() {
    ScrollResult scroll = new ScrollResult();
    SearchEntity hit = new SearchEntity();
    hit.setEntity(PRODUCT_URN);
    scroll.setEntities(new SearchEntityArray(List.of(hit)));
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any())).thenReturn(scroll);
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenThrow(new RuntimeException("timeout"));

    List<MCPItem> output = run(domainKeyMcl(ChangeType.DELETE));

    assertTrue(output.isEmpty());
    verify(mockAspectRetriever, times(DomainReferenceDetachSideEffect.MAX_PAGE_ATTEMPTS))
        .getLatestAspectObjects(any(), any(), any());
  }

  @Test
  public void testStaleSearchHitProducesNoPatch() {
    ScrollResult scroll = new ScrollResult();
    SearchEntity hit = new SearchEntity();
    hit.setEntity(PRODUCT_URN);
    scroll.setEntities(new SearchEntityArray(List.of(hit)));
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any())).thenReturn(scroll);
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(
            Map.of(
                PRODUCT_URN,
                Map.of(
                    DOMAINS_ASPECT_NAME,
                    new Aspect(domainsAspect(UrnUtils.getUrn("urn:li:domain:other")).data()))));

    List<MCPItem> output = run(domainKeyMcl(ChangeType.DELETE));
    assertTrue(output.isEmpty());
  }

  @Test
  public void testNonDeleteEmitsNothing() {
    List<MCPItem> output = run(domainKeyMcl(ChangeType.UPSERT));
    assertTrue(output.isEmpty());
  }

  @Test
  public void testDatasetHitIsEligibleWhenRegistrySupportsDomains() {
    ScrollResult scroll = new ScrollResult();
    SearchEntity hit = new SearchEntity();
    hit.setEntity(DATASET_URN);
    scroll.setEntities(new SearchEntityArray(List.of(hit)));
    when(mockSearchRetriever.scroll(any(), any(), any(), any(), any(), any())).thenReturn(scroll);
    when(mockAspectRetriever.getLatestAspectObjects(any(), any(), any()))
        .thenReturn(
            Map.of(
                DATASET_URN,
                Map.of(DOMAINS_ASPECT_NAME, new Aspect(domainsAspect(DOMAIN_URN).data()))));

    List<MCPItem> output = run(domainKeyMcl(ChangeType.DELETE));

    assertEquals(output.size(), 1);
    assertRemoveDomainPatch(output.get(0), DATASET_URN);
    assertTrue(
        TEST_REGISTRY.getEntitySpec(DATASET_ENTITY_NAME).getAspectSpec(DOMAINS_ASPECT_NAME)
            != null);
    assertTrue(
        TEST_REGISTRY.getEntitySpec(DATA_PRODUCT_ENTITY_NAME).getAspectSpec(DOMAINS_ASPECT_NAME)
            != null);
  }

  private static void assertRemoveDomainPatch(MCPItem item, Urn entityUrn) {
    assertEquals(item.getUrn(), entityUrn);
    assertEquals(item.getAspectName(), DOMAINS_ASPECT_NAME);
    assertEquals(item.getChangeType(), ChangeType.PATCH);
    assertTrue(item instanceof PatchItemImpl);
    String patchJson = ((PatchItemImpl) item).getPatch().toString();
    assertTrue(patchJson.contains("remove"));
    assertTrue(patchJson.contains("/domains/" + DOMAIN_URN));
  }
}
