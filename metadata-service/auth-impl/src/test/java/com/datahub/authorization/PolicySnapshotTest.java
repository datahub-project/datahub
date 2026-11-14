package com.datahub.authorization;

import static org.testng.Assert.*;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

public class PolicySnapshotTest {

  private static DataHubPolicyInfo policy(String name, Long lastUpdated, String... privileges) {
    DataHubPolicyInfo info =
        new DataHubPolicyInfo()
            .setDisplayName(name)
            .setType("METADATA")
            .setState("ACTIVE")
            .setPrivileges(new StringArray(Arrays.asList(privileges)));
    if (lastUpdated != null) {
      info.setLastUpdatedTimestamp(lastUpdated);
    }
    return info;
  }

  private static PolicyFetcher.Policy entry(String id, DataHubPolicyInfo info) {
    return new PolicyFetcher.Policy(UrnUtils.getUrn("urn:li:dataHubPolicy:" + id), info);
  }

  @Test
  public void testEmptySnapshotReturnsDefault() {
    List<DataHubPolicyInfo> dflt = new ArrayList<>();
    assertSame(PolicySnapshot.EMPTY.getOrDefault("EDIT_ENTITY", dflt), dflt);
    assertSame(PolicySnapshot.EMPTY.getOrDefault(DataHubAuthorizer.ALL, dflt), dflt);
    assertEquals(PolicySnapshot.EMPTY.size(), 0);
  }

  @Test
  public void testEachPolicyIsIndexedUnderEachPrivilegeAndAll() {
    DataHubPolicyInfo p1 = policy("p1", 3L, "EDIT_ENTITY", "MANAGE_POLICIES");
    DataHubPolicyInfo p2 = policy("p2", 2L, "EDIT_ENTITY_DOCS");
    DataHubPolicyInfo p3 = policy("p3", 1L, "CREATE_ENTITY", "EDIT_ENTITY");

    PolicySnapshot snapshot =
        PolicySnapshot.of(List.of(entry("a", p1), entry("b", p2), entry("c", p3)));

    assertEquals(snapshot.size(), 3);
    assertEquals(snapshot.getOrDefault("EDIT_ENTITY", List.of()), List.of(p1, p3));
    assertEquals(snapshot.getOrDefault("MANAGE_POLICIES", List.of()), List.of(p1));
    assertEquals(snapshot.getOrDefault("EDIT_ENTITY_DOCS", List.of()), List.of(p2));
    assertEquals(snapshot.getOrDefault("CREATE_ENTITY", List.of()), List.of(p3));
    assertEquals(snapshot.getOrDefault(DataHubAuthorizer.ALL, List.of()), List.of(p1, p2, p3));

    // The same object is shared between keys; nothing is copied.
    assertSame(snapshot.getOrDefault("EDIT_ENTITY", List.of()).get(1), p3);
    assertSame(snapshot.getOrDefault("CREATE_ENTITY", List.of()).get(0), p3);
  }

  @Test
  public void testInactivePoliciesAreKept() {
    DataHubPolicyInfo inactive = policy("old", 1L, "EDIT_ENTITY").setState("INACTIVE");
    PolicySnapshot snapshot = PolicySnapshot.of(List.of(entry("old", inactive)));
    assertEquals(snapshot.getOrDefault("EDIT_ENTITY", List.of()), List.of(inactive));
  }

  @Test
  public void testOrderIsNewestFirstThenMissingTimestampThenUrn() {
    DataHubPolicyInfo newest = policy("newest", 300L, "EDIT_ENTITY");
    DataHubPolicyInfo middle = policy("middle", 200L, "EDIT_ENTITY");
    DataHubPolicyInfo tieB = policy("tieB", 100L, "EDIT_ENTITY");
    DataHubPolicyInfo tieA = policy("tieA", 100L, "EDIT_ENTITY");
    DataHubPolicyInfo noTimeZ = policy("noTimeZ", null, "EDIT_ENTITY");
    DataHubPolicyInfo noTimeY = policy("noTimeY", null, "EDIT_ENTITY");

    // Input order is deliberately mixed.
    PolicySnapshot snapshot =
        PolicySnapshot.of(
            List.of(
                entry("z", noTimeZ),
                entry("tie-b", tieB),
                entry("m", middle),
                entry("y", noTimeY),
                entry("tie-a", tieA),
                entry("n", newest)));

    List<DataHubPolicyInfo> expected = List.of(newest, middle, tieA, tieB, noTimeY, noTimeZ);
    assertEquals(snapshot.getOrDefault("EDIT_ENTITY", List.of()), expected);
    assertEquals(snapshot.getOrDefault(DataHubAuthorizer.ALL, List.of()), expected);
  }

  @Test
  public void testEntriesWithoutUrnKeepTheirInputOrderOnTies() {
    DataHubPolicyInfo first = policy("first", null, "EDIT_ENTITY");
    DataHubPolicyInfo second = policy("second", null, "EDIT_ENTITY");
    PolicySnapshot snapshot = PolicySnapshot.ofPolicyInfos(List.of(first, second));
    assertEquals(snapshot.getOrDefault("EDIT_ENTITY", List.of()), List.of(first, second));
  }

  @Test
  public void testSnapshotIsImmutable() {
    PolicySnapshot snapshot =
        PolicySnapshot.of(List.of(entry("a", policy("p1", 1L, "EDIT_ENTITY"))));
    List<DataHubPolicyInfo> list = snapshot.getOrDefault("EDIT_ENTITY", List.of());
    assertThrows(
        UnsupportedOperationException.class, () -> list.add(policy("x", 2L, "EDIT_ENTITY")));
    assertThrows(UnsupportedOperationException.class, () -> list.remove(0));
  }

  @Test
  public void testPoliciesWithEmptyPrivilegesAppearOnlyUnderAll() {
    DataHubPolicyInfo none = policy("none", 1L);
    PolicySnapshot snapshot = PolicySnapshot.of(List.of(entry("none", none)));
    assertEquals(snapshot.getOrDefault(DataHubAuthorizer.ALL, List.of()), List.of(none));
    assertNull(snapshot.getOrDefault("EDIT_ENTITY", null));
  }
}
