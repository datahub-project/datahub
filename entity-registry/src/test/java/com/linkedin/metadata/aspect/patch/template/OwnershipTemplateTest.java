package com.linkedin.metadata.aspect.patch.template;

import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.template.common.OwnershipTemplate;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OwnershipTemplateTest {

  private static final OwnershipTemplate TEMPLATE = new OwnershipTemplate();

  /** Builds an attributed Owner entry. */
  private static Owner attributedOwner(String ownerUrn, OwnershipType type, String sourceUrn) {
    return new Owner()
        .setOwner(UrnUtils.getUrn(ownerUrn))
        .setType(type)
        .setAttribution(
            new MetadataAttribution()
                .setSource(UrnUtils.getUrn(sourceUrn))
                .setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"))
                .setTime(0L));
  }

  @Test
  public void testAddTwoDistinctEntries() throws Exception {
    Ownership initial = new Ownership();
    initial.setOwners(new OwnerArray());

    JsonPatch patchA =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/owners/urn:li:corpuser:userA/DATAOWNER//")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("owner", "urn:li:corpuser:userA")
                                        .add("type", "DATAOWNER"))))
                .build());

    JsonPatch patchB =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/owners/urn:li:corpuser:userB/DATAOWNER//")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("owner", "urn:li:corpuser:userB")
                                        .add("type", "DATAOWNER"))))
                .build());

    Ownership afterA = TEMPLATE.applyPatch(initial, patchA);
    Ownership result = TEMPLATE.applyPatch(afterA, patchB);

    Assert.assertNotNull(result.getOwners());
    Assert.assertEquals(result.getOwners().size(), 2);
    List<String> ownerUrns =
        result.getOwners().stream().map(o -> o.getOwner().toString()).collect(Collectors.toList());
    Assert.assertTrue(ownerUrns.contains("urn:li:corpuser:userA"), "userA should be present");
    Assert.assertTrue(ownerUrns.contains("urn:li:corpuser:userB"), "userB should be present");
  }

  @Test
  public void testRemoveOneOfTwoEntries() throws Exception {
    Ownership initial = new Ownership();
    initial.setOwners(
        new OwnerArray(
            new Owner()
                .setOwner(UrnUtils.getUrn("urn:li:corpuser:userA"))
                .setType(OwnershipType.DATAOWNER),
            new Owner()
                .setOwner(UrnUtils.getUrn("urn:li:corpuser:userB"))
                .setType(OwnershipType.DATAOWNER)));

    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/owners/urn:li:corpuser:userA"))
                .build());

    Ownership result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getOwners());
    Assert.assertEquals(result.getOwners().size(), 1);
    List<String> ownerUrns =
        result.getOwners().stream().map(o -> o.getOwner().toString()).collect(Collectors.toList());
    Assert.assertFalse(ownerUrns.contains("urn:li:corpuser:userA"), "userA should be removed");
    Assert.assertTrue(ownerUrns.contains("urn:li:corpuser:userB"), "userB should remain");
  }

  @Test
  public void testSameOwnerSameTypeDifferentTypeUrnsCoexist() throws Exception {
    // (jdoe, DATAOWNER, typeA) and (jdoe, DATAOWNER, typeB) must coexist since typeUrn differs.
    String JDOE = "urn:li:corpuser:jdoe";
    String TYPE_A = "urn:li:ownershipType:Technical";
    String TYPE_B = "urn:li:ownershipType:Business";

    Ownership initial = new Ownership();
    initial.setOwners(new OwnerArray());

    JsonPatch patchA =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/owners/" + JDOE + "/DATAOWNER/" + TYPE_A + "/")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("owner", JDOE)
                                        .add("type", "DATAOWNER")
                                        .add("typeUrn", TYPE_A))))
                .build());

    JsonPatch patchB =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/owners/" + JDOE + "/DATAOWNER/" + TYPE_B + "/")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("owner", JDOE)
                                        .add("type", "DATAOWNER")
                                        .add("typeUrn", TYPE_B))))
                .build());

    Ownership afterA = TEMPLATE.applyPatch(initial, patchA);
    Ownership result = TEMPLATE.applyPatch(afterA, patchB);

    Assert.assertEquals(result.getOwners().size(), 2, "both typeUrn entries should coexist");
    List<String> typeUrns =
        result.getOwners().stream()
            .map(o -> o.getTypeUrn().toString())
            .collect(Collectors.toList());
    Assert.assertTrue(typeUrns.contains(TYPE_A), "typeA entry must be present");
    Assert.assertTrue(typeUrns.contains(TYPE_B), "typeB entry must be present");
  }

  @Test
  public void testUnattributedAddToDoesNotUpsert() throws Exception {
    String USER_A = "urn:li:corpuser:userA";
    String SRC_A = "urn:li:dataHubAction:srcA";
    String SRC_B = "urn:li:dataHubAction:srcB";

    Ownership empty = new Ownership().setOwners(new OwnerArray());

    // Step 1: patch-add (userA, DATAOWNER, srcA).
    JsonPatch patchSrcA =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/owners/" + USER_A + "/DATAOWNER//" + SRC_A)
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("owner", USER_A)
                                        .add("type", "DATAOWNER")
                                        .add(
                                            "attribution",
                                            Json.createObjectBuilder()
                                                .add("source", SRC_A)
                                                .add("actor", "urn:li:corpuser:datahub")
                                                .add("time", 0)))))
                .build());

    // Step 2: patch-add (userA, DATAOWNER, srcB) — same owner+type+typeUrn, different attribution.
    JsonPatch patchSrcB =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/owners/" + USER_A + "/DATAOWNER//" + SRC_B)
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("owner", USER_A)
                                        .add("type", "DATAOWNER")
                                        .add(
                                            "attribution",
                                            Json.createObjectBuilder()
                                                .add("source", SRC_B)
                                                .add("actor", "urn:li:corpuser:datahub")
                                                .add("time", 0)))))
                .build());

    Ownership withTwo = TEMPLATE.applyPatch(TEMPLATE.applyPatch(empty, patchSrcA), patchSrcB);

    Assert.assertEquals(withTwo.getOwners().size(), 2, "both attributed entries must coexist");
    Assert.assertEquals(
        withTwo.getOwners().stream()
            .map(o -> o.getAttribution().getSource().toString())
            .collect(Collectors.toSet()),
        Set.of(SRC_A, SRC_B),
        "both sources must be present");

    // Step 3: patch-add unattributed (userA, DATAOWNER, attribution="") — must not displace either.
    JsonPatch patchUnattributed =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/owners/" + USER_A + "/DATAOWNER//")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("owner", USER_A)
                                        .add("type", "DATAOWNER"))))
                .build());

    Ownership result = TEMPLATE.applyPatch(withTwo, patchUnattributed);

    Assert.assertNotNull(result.getOwners());
    Assert.assertEquals(
        result.getOwners().size(), 3, "both attributed + unattributed must coexist");
    long attributed = result.getOwners().stream().filter(o -> o.getAttribution() != null).count();
    long unattributed = result.getOwners().stream().filter(o -> o.getAttribution() == null).count();
    Assert.assertEquals(attributed, 2L, "both attributed entries must survive");
    Assert.assertEquals(unattributed, 1L, "unattributed entry must be added");
  }

  @Test
  public void testUnattributedAddPreservesAttributedDuplicates() throws Exception {
    // Start with (srcA, userA, DATAOWNER) and (srcB, userA, PRODUCER) — two attributed entries
    // for the same owner URN.
    Ownership initial = new Ownership();
    initial.setOwners(
        new OwnerArray(
            attributedOwner(
                "urn:li:corpuser:userA", OwnershipType.DATAOWNER, "urn:li:dataHubAction:srcA"),
            attributedOwner(
                "urn:li:corpuser:userA", OwnershipType.PRODUCER, "urn:li:dataHubAction:srcB")));

    // Plain patch: add userB (no attribution).
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "add")
                        .add("path", "/owners/urn:li:corpuser:userB/DATAOWNER//")
                        .add(
                            "value",
                            Json.createArrayBuilder()
                                .add(
                                    Json.createObjectBuilder()
                                        .add("owner", "urn:li:corpuser:userB")
                                        .add("type", "DATAOWNER"))))
                .build());

    Ownership result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getOwners());
    List<String> ownerUrns =
        result.getOwners().stream().map(o -> o.getOwner().toString()).collect(Collectors.toList());
    // Both attributed userA entries should survive, plus the new userB.
    long userACount = ownerUrns.stream().filter("urn:li:corpuser:userA"::equals).count();
    long userBCount = ownerUrns.stream().filter("urn:li:corpuser:userB"::equals).count();
    Assert.assertEquals(userACount, 2L, "both attributed userA entries should be preserved");
    Assert.assertEquals(userBCount, 1L, "new userB entry should be present");
    Assert.assertEquals(result.getOwners().size(), 3);
  }

  @Test
  public void testUnattributedRemoveDeletesAllEntriesForOwner() throws Exception {
    // (srcA, userA, DATAOWNER), (srcB, userA, PRODUCER), (srcC, userB, DATAOWNER)
    Ownership initial = new Ownership();
    initial.setOwners(
        new OwnerArray(
            attributedOwner(
                "urn:li:corpuser:userA", OwnershipType.DATAOWNER, "urn:li:dataHubAction:srcA"),
            attributedOwner(
                "urn:li:corpuser:userA", OwnershipType.PRODUCER, "urn:li:dataHubAction:srcB"),
            attributedOwner(
                "urn:li:corpuser:userB", OwnershipType.DATAOWNER, "urn:li:dataHubAction:srcC")));

    // Plain remove of userA — should delete the entire list at key userA.
    JsonPatch patch =
        Json.createPatch(
            Json.createArrayBuilder()
                .add(
                    Json.createObjectBuilder()
                        .add("op", "remove")
                        .add("path", "/owners/urn:li:corpuser:userA"))
                .build());

    Ownership result = TEMPLATE.applyPatch(initial, patch);

    Assert.assertNotNull(result.getOwners());
    List<String> ownerUrns =
        result.getOwners().stream().map(o -> o.getOwner().toString()).collect(Collectors.toList());
    Assert.assertFalse(
        ownerUrns.contains("urn:li:corpuser:userA"), "all userA entries should be gone");
    Assert.assertTrue(ownerUrns.contains("urn:li:corpuser:userB"), "userB entry should survive");
    Assert.assertEquals(result.getOwners().size(), 1);
    Owner survivor = result.getOwners().get(0);
    Assert.assertNotNull(survivor.getAttribution());
    Assert.assertEquals(
        survivor.getAttribution().getSource().toString(), "urn:li:dataHubAction:srcC");
  }
}
