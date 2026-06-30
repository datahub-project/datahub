package com.datahub.authorization;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.Test;

public class SessionActorIdentityTest {

  private static final Urn ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:testuser");
  private static final Urn GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:test");
  private static final Urn DIRECT_ROLE = UrnUtils.getUrn("urn:li:dataHubRole:Editor");
  private static final Urn INHERITED_ROLE = UrnUtils.getUrn("urn:li:dataHubRole:Admin");

  @Test
  public void empty_createsIdentityWithNoGroupsOrRoles() {
    SessionActorIdentity identity = SessionActorIdentity.empty(ACTOR_URN);

    assertEquals(identity.getActorUrn(), ACTOR_URN);
    assertTrue(identity.getGroups().isEmpty());
    assertTrue(identity.getDirectRoles().isEmpty());
  }

  @Test
  public void constructor_copiesGroupAndRoleCollections() {
    List<Urn> mutableGroups = new ArrayList<>(List.of(GROUP_URN));
    Set<Urn> mutableRoles = Set.of(DIRECT_ROLE);

    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, mutableGroups, mutableRoles);
    mutableGroups.add(UrnUtils.getUrn("urn:li:corpGroup:other"));

    assertEquals(identity.getGroups(), List.of(GROUP_URN));
    assertEquals(identity.getDirectRoles(), Set.of(DIRECT_ROLE));
  }

  @Test
  public void resolveAllRoles_returnsDirectRolesWhenNoGroups() {
    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, List.of(), Set.of(DIRECT_ROLE));
    AtomicInteger fetchCount = new AtomicInteger();

    Set<Urn> roles =
        identity.resolveAllRoles(
            groups -> {
              fetchCount.incrementAndGet();
              return Set.of(INHERITED_ROLE);
            });

    assertEquals(roles, Set.of(DIRECT_ROLE));
    assertEquals(fetchCount.get(), 0);
  }

  @Test
  public void resolveAllRoles_includesInheritedRolesFromGroups() {
    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, List.of(GROUP_URN), Set.of(DIRECT_ROLE));

    Set<Urn> roles = identity.resolveAllRoles(groups -> Set.of(INHERITED_ROLE));

    assertEquals(roles, Set.of(DIRECT_ROLE, INHERITED_ROLE));
  }

  @Test
  public void resolveAllRoles_invokesFetcherOnceWhenCalledMultipleTimes() {
    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, List.of(GROUP_URN), Set.of(DIRECT_ROLE));
    AtomicInteger fetchCount = new AtomicInteger();

    Set<Urn> first =
        identity.resolveAllRoles(
            groups -> {
              fetchCount.incrementAndGet();
              return Set.of(INHERITED_ROLE);
            });
    Set<Urn> second =
        identity.resolveAllRoles(
            groups -> {
              fetchCount.incrementAndGet();
              return Set.of(INHERITED_ROLE);
            });

    assertEquals(first, second);
    assertEquals(fetchCount.get(), 1);
  }

  @Test
  public void mergeGroupMembership_deduplicatesCorpAndNativeGroups() {
    Urn sharedGroup = UrnUtils.getUrn("urn:li:corpGroup:shared");
    Urn corpOnly = UrnUtils.getUrn("urn:li:corpGroup:corp");
    Urn nativeOnly = UrnUtils.getUrn("urn:li:corpGroup:native");

    List<Urn> merged =
        SessionActorIdentity.mergeGroupMembership(
            List.of(sharedGroup, corpOnly), List.of(sharedGroup, nativeOnly));

    assertEquals(merged.size(), 3);
    assertTrue(merged.containsAll(List.of(sharedGroup, corpOnly, nativeOnly)));
  }

  @Test
  public void constructor_retainsCorpAndNativeGroupsSeparately() {
    Urn corpGroup = UrnUtils.getUrn("urn:li:corpGroup:corp");
    Urn nativeGroup = UrnUtils.getUrn("urn:li:corpGroup:native");

    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, List.of(corpGroup), List.of(nativeGroup), Set.of());

    assertEquals(identity.getCorpGroups(), List.of(corpGroup));
    assertEquals(identity.getNativeGroups(), List.of(nativeGroup));
    assertEquals(identity.getGroups(), List.of(corpGroup, nativeGroup));
  }

  @Test
  public void legacyConstructor_treatsAllGroupsAsCorp() {
    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, List.of(GROUP_URN), Set.of(DIRECT_ROLE));

    assertEquals(identity.getCorpGroups(), List.of(GROUP_URN));
    assertTrue(identity.getNativeGroups().isEmpty());
    assertEquals(identity.getGroups(), List.of(GROUP_URN));
  }
}
