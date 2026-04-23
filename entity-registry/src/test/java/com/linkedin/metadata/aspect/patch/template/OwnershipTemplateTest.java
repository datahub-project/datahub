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
                        .add("path", "/owners/urn:li:corpuser:userA/DATAOWNER")
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
                        .add("path", "/owners/urn:li:corpuser:userB/DATAOWNER")
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
