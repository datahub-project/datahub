package com.linkedin.datahub.graphql.resolvers.domain;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.IntegerArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainLineageInput;
import com.linkedin.datahub.graphql.generated.DomainLineageRelationship;
import com.linkedin.datahub.graphql.generated.DomainLineageResult;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Behavioural tests for {@link DomainLineageResolver}. */
public class DomainLineageResolverTest {

  private static final String SOURCE_DOMAIN_URN = "urn:li:domain:source";
  private static final String NEIGHBOUR_DOMAIN_A = "urn:li:domain:neighbourA";
  private static final String NEIGHBOUR_DOMAIN_B = "urn:li:domain:neighbourB";
  private static final String MEMBER_1 = "urn:li:dataset:(urn:li:dataPlatform:foo,member1,PROD)";
  private static final String MEMBER_2 = "urn:li:dataset:(urn:li:dataPlatform:foo,member2,PROD)";
  private static final String NEIGHBOUR_1 = "urn:li:dataset:(urn:li:dataPlatform:foo,nbr1,PROD)";
  private static final String NEIGHBOUR_2 = "urn:li:dataset:(urn:li:dataPlatform:foo,nbr2,PROD)";

  private EntityClient entityClient;
  private GraphClient graphClient;
  private RestrictedService restrictedService;
  private AuthorizationConfiguration authConfig;
  private DataFetchingEnvironment env;
  private DomainLineageResolver resolver;

  // Per-member hits keyed by source URN; resolved via a single thenAnswer stub so the
  // resolver's parallel fan-out doesn't race Mockito's stub bookkeeping.
  private Map<Urn, LineageSearchResult> hitsByMember;

  @BeforeMethod
  public void setUp() throws Exception {
    entityClient = mock(EntityClient.class);
    graphClient = mock(GraphClient.class);
    restrictedService = mock(RestrictedService.class);

    authConfig = mock(AuthorizationConfiguration.class);
    ViewAuthorizationConfiguration viewConfig = mock(ViewAuthorizationConfiguration.class);
    when(viewConfig.isEnabled()).thenReturn(false);
    when(authConfig.getView()).thenReturn(viewConfig);

    env = mock(DataFetchingEnvironment.class);
    Domain sourceDomain = new Domain();
    sourceDomain.setUrn(SOURCE_DOMAIN_URN);
    QueryContext queryContext = getMockAllowContext();
    when(env.getSource()).thenReturn(sourceDomain);
    when(env.getContext()).thenReturn(queryContext);

    hitsByMember = new HashMap<>();
    when(entityClient.searchAcrossLineage(
            any(),
            any(),
            any(),
            anyList(),
            anyString(),
            anyInt(),
            any(),
            anyList(),
            anyInt(),
            anyInt()))
        .thenAnswer(
            invocation -> {
              Urn memberUrn = invocation.getArgument(1);
              return hitsByMember.getOrDefault(memberUrn, emptyLineageResult());
            });

    resolver = new DomainLineageResolver(entityClient, graphClient, restrictedService, authConfig);
  }

  private static LineageSearchResult emptyLineageResult() {
    return new LineageSearchResult().setEntities(new LineageSearchEntityArray()).setNumEntities(0);
  }

  @Test
  public void testEmptyMembersReturnsEmptyResult() throws Exception {
    setMembers(); // zero members
    when(env.getArgument(eq("input"))).thenReturn(downstreamInput());

    DomainLineageResult result = resolver.get(env).join();

    assertEquals(result.getTotal(), 0);
    assertEquals(result.getCount(), 0);
    assertEquals(result.getMemberScanCount(), 0);
    assertEquals(result.getMemberTotal(), 0);
    assertFalse(result.getIsPartial());
    assertTrue(result.getRelationships().isEmpty());
  }

  @Test
  public void testSingleMemberSingleNeighbourBucketsToOwnerDomain() throws Exception {
    setMembers(MEMBER_1);
    setLineageHits(MEMBER_1, hit(NEIGHBOUR_1, 1));
    setDomainOwners(NEIGHBOUR_1, NEIGHBOUR_DOMAIN_A);

    when(env.getArgument(eq("input"))).thenReturn(downstreamInput());

    DomainLineageResult result = resolver.get(env).join();

    assertEquals(result.getTotal(), 1);
    assertEquals(result.getCount(), 1);
    assertFalse(result.getIsPartial());
    DomainLineageRelationship rel = result.getRelationships().get(0);
    assertEquals(rel.getEntity().getUrn(), NEIGHBOUR_DOMAIN_A);
    assertEquals(rel.getMemberMatchCount(), 1);
    assertEquals(rel.getNeighbourEntityCount(), 1);
    assertEquals(rel.getDegreeMin(), 1);
    assertEquals(rel.getDegreeMax(), 1);
    assertEquals(rel.getDirection(), LineageDirection.DOWNSTREAM);
  }

  @Test
  public void testMultipleMembersAggregateIntoSameOwnerBucket() throws Exception {
    setMembers(MEMBER_1, MEMBER_2);
    // Both members reach NEIGHBOUR_1 (same owner) → memberMatchCount must be 2, neighbourCount 1.
    setLineageHits(MEMBER_1, hit(NEIGHBOUR_1, 1));
    setLineageHits(MEMBER_2, hit(NEIGHBOUR_1, 2));
    setDomainOwners(NEIGHBOUR_1, NEIGHBOUR_DOMAIN_A);

    when(env.getArgument(eq("input"))).thenReturn(downstreamInput());

    DomainLineageResult result = resolver.get(env).join();

    assertEquals(result.getTotal(), 1);
    DomainLineageRelationship rel = result.getRelationships().get(0);
    assertEquals(rel.getMemberMatchCount(), 2);
    assertEquals(rel.getNeighbourEntityCount(), 1);
    assertEquals(rel.getDegreeMin(), 1);
    assertEquals(rel.getDegreeMax(), 2);
  }

  @Test
  public void testNeighbourWithNoResolvableOwnerIsDroppedAndIsPartialSet() throws Exception {
    setMembers(MEMBER_1);
    setLineageHits(MEMBER_1, hit(NEIGHBOUR_1, 1), hit(NEIGHBOUR_2, 1));
    setDomainOwners(NEIGHBOUR_1, NEIGHBOUR_DOMAIN_A);
    // NEIGHBOUR_2 deliberately has no owner mapping → must be dropped.

    when(env.getArgument(eq("input"))).thenReturn(downstreamInput());

    DomainLineageResult result = resolver.get(env).join();

    assertEquals(result.getTotal(), 1);
    assertTrue(result.getIsPartial(), "ownerless neighbour must flip isPartial");
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), NEIGHBOUR_DOMAIN_A);
  }

  @Test
  public void testMultiMembershipNeighbourPopulatesBothOwners() throws Exception {
    setMembers(MEMBER_1);
    setLineageHits(MEMBER_1, hit(NEIGHBOUR_1, 1));
    setDomainOwners(NEIGHBOUR_1, NEIGHBOUR_DOMAIN_A, NEIGHBOUR_DOMAIN_B);

    when(env.getArgument(eq("input"))).thenReturn(downstreamInput());

    DomainLineageResult result = resolver.get(env).join();

    assertEquals(result.getTotal(), 2);
    Set<String> seenOwners = new HashSet<>();
    for (DomainLineageRelationship rel : result.getRelationships()) {
      seenOwners.add(rel.getEntity().getUrn());
      assertEquals(rel.getMemberMatchCount(), 1);
      assertEquals(rel.getNeighbourEntityCount(), 1);
    }
    assertTrue(seenOwners.contains(NEIGHBOUR_DOMAIN_A));
    assertTrue(seenOwners.contains(NEIGHBOUR_DOMAIN_B));
  }

  @Test
  public void testSelfLoopOwnerIsSuppressed() throws Exception {
    setMembers(MEMBER_1);
    setLineageHits(MEMBER_1, hit(NEIGHBOUR_1, 1));
    // Neighbour resolves back to the source Domain itself — must NOT show up as a relationship.
    setDomainOwners(NEIGHBOUR_1, SOURCE_DOMAIN_URN);

    when(env.getArgument(eq("input"))).thenReturn(downstreamInput());

    DomainLineageResult result = resolver.get(env).join();

    assertEquals(result.getTotal(), 0);
    assertTrue(result.getRelationships().isEmpty());
  }

  @Test
  public void testMemberScanCapExceededFlipsIsPartial() throws Exception {
    setMembersWithTotal(2, /* total= */ 50);
    setLineageHits(MEMBER_1, hit(NEIGHBOUR_1, 1));
    setLineageHits(MEMBER_2, hit(NEIGHBOUR_1, 1));
    setDomainOwners(NEIGHBOUR_1, NEIGHBOUR_DOMAIN_A);

    DomainLineageInput input = downstreamInput();
    input.setMemberScanCap(2);
    when(env.getArgument(eq("input"))).thenReturn(input);

    DomainLineageResult result = resolver.get(env).join();

    assertEquals(result.getMemberScanCount(), 2);
    assertEquals(result.getMemberTotal(), 50);
    assertTrue(result.getIsPartial(), "memberScanCap exceeded must flip isPartial");
  }

  @Test
  public void testPaginationSlicesAfterSorting() throws Exception {
    // Three owners — A has 2 hits, B has 1 hit, C has 1 hit. Sorted by memberMatchCount desc,
    // tie-broken by ownerUrn ascending: [A(2), B(1), C(1)]. Request start=1, count=1 → [B].
    setMembers(MEMBER_1, MEMBER_2);
    String neighbourC = "urn:li:dataset:(urn:li:dataPlatform:foo,nbrC,PROD)";
    String ownerA = "urn:li:domain:aaa";
    String ownerB = "urn:li:domain:bbb";
    String ownerC = "urn:li:domain:ccc";
    setLineageHits(MEMBER_1, hit(NEIGHBOUR_1, 1), hit(NEIGHBOUR_2, 1));
    setLineageHits(MEMBER_2, hit(NEIGHBOUR_1, 1), hit(neighbourC, 1));
    Map<String, String[]> ownerMap = new HashMap<>();
    ownerMap.put(NEIGHBOUR_1, new String[] {ownerA});
    ownerMap.put(NEIGHBOUR_2, new String[] {ownerB});
    ownerMap.put(neighbourC, new String[] {ownerC});
    setDomainOwners(ownerMap);

    DomainLineageInput input = downstreamInput();
    input.setStart(1);
    input.setCount(1);
    when(env.getArgument(eq("input"))).thenReturn(input);

    DomainLineageResult result = resolver.get(env).join();

    assertEquals(result.getTotal(), 3);
    assertEquals(result.getStart(), 1);
    assertEquals(result.getCount(), 1);
    assertEquals(result.getRelationships().size(), 1);
    assertEquals(
        result.getRelationships().get(0).getEntity().getUrn(),
        ownerB,
        "Sort order: A(memberMatchCount=2) > B(1) > C(1) by URN asc tiebreak → page [1,2) is B");
  }

  @Test
  public void testTooManyDistinctNeighboursTruncatesAndFlipsIsPartial() throws Exception {
    // One member produces > MAX_OWNER_LOOKUP_NEIGHBOURS (2000) distinct neighbours, each owned by
    // its own neighbour Domain. The resolver must truncate to the top N (keeping the
    // most-contributing first; here all have equal contribution so it's URN-asc tiebreak) and flip
    // isPartial. perMemberCount must be raised above the cap for this test to actually exercise
    // the truncation path.
    setMembers(MEMBER_1);
    int totalNeighbours = 2100;
    LineageSearchEntity[] hits = new LineageSearchEntity[totalNeighbours];
    Map<String, String[]> ownerMap = new HashMap<>();
    for (int i = 0; i < totalNeighbours; i++) {
      String nbrUrn = String.format("urn:li:dataset:(urn:li:dataPlatform:foo,nbr-%04d,PROD)", i);
      String ownerUrn = String.format("urn:li:domain:owner-%04d", i);
      hits[i] = hit(nbrUrn, 1);
      ownerMap.put(nbrUrn, new String[] {ownerUrn});
    }
    setLineageHits(MEMBER_1, hits);
    setDomainOwners(ownerMap);

    DomainLineageInput input = downstreamInput();
    input.setPerMemberCount(totalNeighbours);
    input.setCount(50);
    when(env.getArgument(eq("input"))).thenReturn(input);

    DomainLineageResult result = resolver.get(env).join();

    assertTrue(result.getIsPartial(), "neighbour truncation must flip isPartial");
    // The cap is MAX_OWNER_LOOKUP_NEIGHBOURS=2000 distinct neighbours; we'd expect ≤2000 total
    // buckets (one owner per neighbour in this test setup).
    assertTrue(
        result.getTotal() <= 2000,
        "result.total must be ≤ MAX_OWNER_LOOKUP_NEIGHBOURS; was " + result.getTotal());
    assertEquals(result.getCount(), 50);
  }

  @Test
  public void testPerMemberLineageFailureToleratedAndOtherMembersStillContribute()
      throws Exception {
    setMembers(MEMBER_1, MEMBER_2);
    setLineageHits(MEMBER_1, hit(NEIGHBOUR_1, 1));
    // MEMBER_2 throws — must not bring the whole resolver down.
    Urn member2Urn = UrnUtils.getUrn(MEMBER_2);
    when(entityClient.searchAcrossLineage(
            any(),
            any(),
            any(),
            anyList(),
            anyString(),
            anyInt(),
            any(),
            anyList(),
            anyInt(),
            anyInt()))
        .thenAnswer(
            invocation -> {
              Urn memberUrn = invocation.getArgument(1);
              if (member2Urn.equals(memberUrn)) {
                throw new RuntimeException("simulated ES failure");
              }
              return hitsByMember.getOrDefault(memberUrn, emptyLineageResult());
            });
    setDomainOwners(NEIGHBOUR_1, NEIGHBOUR_DOMAIN_A);

    when(env.getArgument(eq("input"))).thenReturn(downstreamInput());

    DomainLineageResult result = resolver.get(env).join();

    assertEquals(result.getTotal(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), NEIGHBOUR_DOMAIN_A);
  }

  // ---------------------------------------------------------------------------
  // Mock-wiring helpers
  // ---------------------------------------------------------------------------

  private DomainLineageInput downstreamInput() {
    DomainLineageInput input = new DomainLineageInput();
    input.setDirection(LineageDirection.DOWNSTREAM);
    input.setHops(1);
    input.setStart(0);
    input.setCount(25);
    input.setMemberScanCap(1000);
    input.setPerMemberCount(100);
    input.setGroupByDataProduct(false);
    input.setIncludeRestricted(true);
    return input;
  }

  private void setMembers(String... memberUrns) throws Exception {
    setMembersWithTotal(memberUrns.length, memberUrns.length, memberUrns);
  }

  private void setMembersWithTotal(int returned, int total, String... memberUrns) throws Exception {
    String[] urns = memberUrns.length == returned ? memberUrns : firstNDefaultMembers(returned);
    SearchEntityArray entities = new SearchEntityArray();
    for (String urn : urns) {
      entities.add(new SearchEntity().setEntity(UrnUtils.getUrn(urn)));
    }
    SearchResult sr = new SearchResult().setEntities(entities).setNumEntities(total);
    when(entityClient.searchAcrossEntities(
            any(), anyList(), anyString(), any(), anyInt(), anyInt(), anyList()))
        .thenReturn(sr);
  }

  private void setMembersWithTotal(int returned, int total) throws Exception {
    setMembersWithTotal(returned, total, firstNDefaultMembers(returned));
  }

  private String[] firstNDefaultMembers(int n) {
    String[] urns = new String[n];
    for (int i = 0; i < n; i++) {
      urns[i] =
          i == 0
              ? MEMBER_1
              : i == 1
                  ? MEMBER_2
                  : "urn:li:dataset:(urn:li:dataPlatform:foo,member" + (i + 1) + ",PROD)";
    }
    return urns;
  }

  /** Wires per-member lineage hits. Multiple calls accumulate (one per member). */
  private void setLineageHits(String memberUrn, LineageSearchEntity... hits) {
    LineageSearchEntityArray array = new LineageSearchEntityArray(Arrays.asList(hits));
    LineageSearchResult lsr =
        new LineageSearchResult().setEntities(array).setNumEntities(hits.length);
    hitsByMember.put(UrnUtils.getUrn(memberUrn), lsr);
  }

  private LineageSearchEntity hit(String neighbourUrn, int degree) {
    IntegerArray degrees = new IntegerArray();
    degrees.add(degree);
    return new LineageSearchEntity().setEntity(UrnUtils.getUrn(neighbourUrn)).setDegrees(degrees);
  }

  private void setDomainOwners(String neighbourUrn, String... ownerUrns) throws Exception {
    Map<String, String[]> single = new HashMap<>();
    single.put(neighbourUrn, ownerUrns);
    setDomainOwners(single);
  }

  @SuppressWarnings("unchecked")
  private void setDomainOwners(Map<String, String[]> ownersByNeighbour) throws Exception {
    Map<Urn, EntityResponse> responses = new HashMap<>();
    for (Map.Entry<String, String[]> entry : ownersByNeighbour.entrySet()) {
      Urn neighbour = UrnUtils.getUrn(entry.getKey());
      Domains domainsAspect = new Domains();
      UrnArray domainArray = new UrnArray();
      for (String owner : entry.getValue()) {
        domainArray.add(UrnUtils.getUrn(owner));
      }
      domainsAspect.setDomains(domainArray);

      EnvelopedAspectMap aspects = new EnvelopedAspectMap();
      aspects.put(
          Constants.DOMAINS_ASPECT_NAME,
          new EnvelopedAspect().setValue(new Aspect(domainsAspect.data())));
      responses.put(neighbour, new EntityResponse().setUrn(neighbour).setAspects(aspects));
    }

    when(entityClient.batchGetV2(any(), anyString(), any(Set.class), any(Set.class)))
        .thenAnswer(
            invocation -> {
              Set<Urn> requested = invocation.getArgument(2);
              Map<Urn, EntityResponse> filtered = new HashMap<>();
              for (Urn u : requested) {
                if (responses.containsKey(u)) {
                  filtered.put(u, responses.get(u));
                }
              }
              return filtered;
            });
  }
}
