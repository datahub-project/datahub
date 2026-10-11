package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.lifecycle.LifecycleStageSettings;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LifecycleStageTypeServiceTest {

  private static final Urn ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:testUser");
  private static final OperationContext OP_CTX =
      TestOperationContexts.userContextNoSearchAuthorization(ACTOR_URN);

  private static final Urn PROPOSED_URN = UrnUtils.getUrn("urn:li:lifecycleStageType:PROPOSED");
  private static final Urn ARCHIVED_URN = UrnUtils.getUrn("urn:li:lifecycleStageType:ARCHIVED");
  private static final Urn VISIBLE_URN = UrnUtils.getUrn("urn:li:lifecycleStageType:VISIBLE");

  private SystemEntityClient entityClient;

  @BeforeMethod
  public void setup() {
    entityClient = mock(SystemEntityClient.class);
  }

  @Test
  public void testGetHiddenStageUrns_entityTypeSpecific() throws Exception {
    // PROPOSED: hideInSearch=true, entityTypes=["document"]
    // ARCHIVED: hideInSearch=true, entityTypes=null (all)
    // VISIBLE: hideInSearch=false
    SearchResult searchResult = makeSearchResult(PROPOSED_URN, ARCHIVED_URN, VISIBLE_URN);
    when(entityClient.search(any(), any(), any(), any(), any(), eq(0), eq(1000)))
        .thenReturn(searchResult);

    Map<Urn, EntityResponse> responses =
        Map.of(
            PROPOSED_URN, makeResponse(PROPOSED_URN, true, new StringArray("document")),
            ARCHIVED_URN, makeResponse(ARCHIVED_URN, true, null),
            VISIBLE_URN, makeResponse(VISIBLE_URN, false, new StringArray("document")));
    when(entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(responses);

    LifecycleStageTypeService service = new LifecycleStageTypeService(entityClient);
    service.setSystemOperationContext(OP_CTX);

    // Querying for "document" should get PROPOSED (entity-specific) + ARCHIVED (global)
    Set<String> hidden = service.getHiddenStageUrns(Set.of("document"));
    assertEquals(hidden.size(), 2);
    assertTrue(hidden.contains(PROPOSED_URN.toString()));
    assertTrue(hidden.contains(ARCHIVED_URN.toString()));

    // Querying for "dataset" should only get ARCHIVED (global)
    Set<String> datasetHidden = service.getHiddenStageUrns(Set.of("dataset"));
    assertEquals(datasetHidden.size(), 1);
    assertTrue(datasetHidden.contains(ARCHIVED_URN.toString()));
  }

  @Test
  public void testGetHiddenStageUrns_emptyEntityTypes() throws Exception {
    // Stage with hideInSearch=true but entityTypes=[] → applies to no types
    SearchResult searchResult = makeSearchResult(PROPOSED_URN);
    when(entityClient.search(any(), any(), any(), any(), any(), eq(0), eq(1000)))
        .thenReturn(searchResult);

    Map<Urn, EntityResponse> responses =
        Map.of(PROPOSED_URN, makeResponse(PROPOSED_URN, true, new StringArray()));
    when(entityClient.batchGetV2(any(), any(), any(), any())).thenReturn(responses);

    LifecycleStageTypeService service = new LifecycleStageTypeService(entityClient);
    service.setSystemOperationContext(OP_CTX);

    Set<String> hidden = service.getHiddenStageUrns(Set.of("document"));
    assertTrue(hidden.isEmpty(), "Empty entityTypes should match no entity types");
  }

  @Test
  public void testGetHiddenStageUrns_noOpContextReturnsEmpty() {
    LifecycleStageTypeService service = new LifecycleStageTypeService(entityClient);
    // Don't set system op context

    Set<String> hidden = service.getHiddenStageUrns(Set.of("document"));
    assertTrue(hidden.isEmpty());
  }

  @Test
  public void testGetHiddenStageUrns_noSearchResultsReturnsEmpty() throws Exception {
    SearchResult emptyResult = new SearchResult();
    emptyResult.setEntities(new SearchEntityArray());
    emptyResult.setFrom(0);
    emptyResult.setPageSize(0);
    emptyResult.setNumEntities(0);
    emptyResult.setMetadata(new SearchResultMetadata());
    when(entityClient.search(any(), any(), any(), any(), any(), eq(0), eq(1000)))
        .thenReturn(emptyResult);

    LifecycleStageTypeService service = new LifecycleStageTypeService(entityClient);
    service.setSystemOperationContext(OP_CTX);

    Set<String> hidden = service.getHiddenStageUrns(Set.of("document"));
    assertTrue(hidden.isEmpty());
  }

  @Test
  public void testGetStageInfo_found() throws Exception {
    EntityResponse response = makeResponse(PROPOSED_URN, true, new StringArray("document"));
    when(entityClient.getV2(any(), eq("lifecycleStageType"), eq(PROPOSED_URN), any()))
        .thenReturn(response);

    LifecycleStageTypeService service = new LifecycleStageTypeService(entityClient);

    LifecycleStageTypeInfo info = service.getStageInfo(OP_CTX, PROPOSED_URN);
    assertNotNull(info);
    assertEquals(info.getName(), "Test Stage");
    assertTrue(info.getSettings().isHideInSearch());
  }

  @Test
  public void testGetStageInfo_notFound() throws Exception {
    when(entityClient.getV2(any(), any(), any(), any())).thenReturn(null);

    LifecycleStageTypeService service = new LifecycleStageTypeService(entityClient);

    assertNull(service.getStageInfo(OP_CTX, PROPOSED_URN));
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────

  private static SearchResult makeSearchResult(Urn... urns) {
    SearchEntityArray entities = new SearchEntityArray();
    for (Urn urn : urns) {
      SearchEntity entity = new SearchEntity();
      entity.setEntity(urn);
      entities.add(entity);
    }
    SearchResult result = new SearchResult();
    result.setEntities(entities);
    result.setFrom(0);
    result.setPageSize(urns.length);
    result.setNumEntities(urns.length);
    result.setMetadata(new SearchResultMetadata());
    return result;
  }

  private static EntityResponse makeResponse(
      Urn urn, boolean hideInSearch, StringArray entityTypes) {
    LifecycleStageSettings settings = new LifecycleStageSettings();
    settings.setHideInSearch(hideInSearch);

    AuditStamp stamp = new AuditStamp();
    stamp.setTime(System.currentTimeMillis());
    stamp.setActor(UrnUtils.getUrn("urn:li:corpuser:system"));

    LifecycleStageTypeInfo info = new LifecycleStageTypeInfo();
    info.setName("Test Stage");
    info.setSettings(settings);
    info.setCreated(stamp);
    info.setLastModified(stamp);
    if (entityTypes != null) {
      info.setEntityTypes(entityTypes);
    }

    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(info.data()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put("lifecycleStageTypeInfo", envelopedAspect);

    EntityResponse response = new EntityResponse();
    response.setUrn(urn);
    response.setEntityName("lifecycleStageType");
    response.setAspects(aspectMap);
    return response;
  }
}
