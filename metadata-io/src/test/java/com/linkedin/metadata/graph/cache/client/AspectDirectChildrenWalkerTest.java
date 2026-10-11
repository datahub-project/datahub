package com.linkedin.metadata.graph.cache.client;

import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.domain.DomainProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectDirectChildrenWalkerTest {

  private static final Urn PARENT = UrnUtils.getUrn("urn:li:domain:parent");
  private static final Urn CHILD = UrnUtils.getUrn("urn:li:domain:child");
  private static final Urn OTHER_PARENT = UrnUtils.getUrn("urn:li:domain:other-parent");

  private OperationContext opContext;
  private EntityClient entityClient;

  @BeforeMethod
  public void setUp() {
    opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    entityClient = mock(EntityClient.class);
  }

  @Test
  public void hasDomainDirectChildrenReturnsTrueWhenPrimaryStoreConfirmsChild()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClient.filter(
            eq(opContext), eq(DOMAIN_ENTITY_NAME), any(), eq(null), eq(0), eq(200)))
        .thenReturn(searchResult(1, CHILD));
    when(entityClient.batchGetV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(CHILD)),
            eq(Set.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(CHILD, domainResponse(CHILD, PARENT)));

    assertTrue(AspectDirectChildrenWalker.hasDomainDirectChildren(opContext, entityClient, PARENT));
  }

  @Test
  public void hasDomainDirectChildrenReturnsFalseWhenSearchStaleButAspectReparented()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClient.filter(
            eq(opContext), eq(DOMAIN_ENTITY_NAME), any(), eq(null), eq(0), eq(200)))
        .thenReturn(searchResult(1, CHILD));
    when(entityClient.batchGetV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(CHILD)),
            eq(Set.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(CHILD, domainResponse(CHILD, OTHER_PARENT)));

    assertFalse(
        AspectDirectChildrenWalker.hasDomainDirectChildren(opContext, entityClient, PARENT));
  }

  @Test
  public void hasDomainDirectChildrenReturnsFalseWhenPrimaryStoreHasNoMatchingChild()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClient.filter(
            eq(opContext), eq(DOMAIN_ENTITY_NAME), any(), eq(null), eq(0), eq(200)))
        .thenReturn(searchResult(1, CHILD));
    when(entityClient.batchGetV2(
            eq(opContext),
            eq(DOMAIN_ENTITY_NAME),
            eq(Set.of(CHILD)),
            eq(Set.of(DOMAIN_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Collections.emptyMap());

    assertFalse(
        AspectDirectChildrenWalker.hasDomainDirectChildren(opContext, entityClient, PARENT));
  }

  @Test
  public void hasDomainDirectChildrenReturnsFalseWhenIndexCountIncludesFilteredGhosts()
      throws RemoteInvocationException, URISyntaxException {
    // Simulates validateSearchResult: ES reports matches but SQL existence filtered all ghosts.
    SearchResult staleIndexCount =
        new SearchResult().setEntities(new SearchEntityArray()).setNumEntities(1);
    when(entityClient.filter(
            eq(opContext), eq(DOMAIN_ENTITY_NAME), any(), eq(null), eq(0), eq(200)))
        .thenReturn(staleIndexCount);

    assertFalse(
        AspectDirectChildrenWalker.hasDomainDirectChildren(opContext, entityClient, PARENT));
    verify(entityClient, never()).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void hasDomainDirectChildrenReturnsTrueWhenSearchPageTruncated()
      throws RemoteInvocationException, URISyntaxException {
    SearchEntityArray entities = new SearchEntityArray();
    for (int i = 0; i < 200; i++) {
      entities.add(new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:domain:child-" + i)));
    }
    SearchResult truncated = new SearchResult().setEntities(entities).setNumEntities(250);
    when(entityClient.filter(
            eq(opContext), eq(DOMAIN_ENTITY_NAME), any(), eq(null), eq(0), eq(200)))
        .thenReturn(truncated);

    assertTrue(AspectDirectChildrenWalker.hasDomainDirectChildren(opContext, entityClient, PARENT));
    verify(entityClient, never()).batchGetV2(any(), any(), any(), any());
  }

  @Test
  public void hasDomainDirectChildrenReturnsFalseWhenNoCandidates()
      throws RemoteInvocationException, URISyntaxException {
    when(entityClient.filter(
            eq(opContext), eq(DOMAIN_ENTITY_NAME), any(), eq(null), eq(0), eq(200)))
        .thenReturn(searchResult(0));

    assertFalse(
        AspectDirectChildrenWalker.hasDomainDirectChildren(opContext, entityClient, PARENT));
  }

  private static SearchResult searchResult(int count, Urn... urns) {
    SearchEntityArray entities = new SearchEntityArray();
    for (Urn urn : urns) {
      entities.add(new SearchEntity().setEntity(urn));
    }
    return new SearchResult().setEntities(entities).setNumEntities(count);
  }

  private static EntityResponse domainResponse(Urn urn, Urn parentUrn) {
    DomainProperties properties = new DomainProperties();
    properties.setParentDomain(parentUrn, com.linkedin.data.template.SetMode.IGNORE_NULL);
    properties.setName("test-domain");
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        DOMAIN_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect()
            .setValue(new Aspect(properties.data()))
            .setCreated(
                new AuditStamp().setTime(0L).setActor(UrnUtils.getUrn("urn:li:corpuser:test"))));
    return new EntityResponse().setUrn(urn).setAspects(aspectMap);
  }
}
