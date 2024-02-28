package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DEFAULT_OWNERSHIP_TYPE_URN;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.UrnArray;
import com.linkedin.common.UrnArrayMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

public class OwnerTypeMapTest {
  private static final AspectRetriever ASPECT_RETRIEVER = mock(AspectRetriever.class);
  private static final EntityRegistry ENTITY_REGISTRY = new TestEntityRegistry();
  private static final AspectPluginConfig ASPECT_PLUGIN_CONFIG =
      AspectPluginConfig.builder()
          .className("some class")
          .enabled(true)
          .supportedEntityAspectNames(
              List.of(
                  AspectPluginConfig.EntityAspectName.builder()
                      .entityName("*")
                      .aspectName("ownership")
                      .build()))
          .build();
  private static final Urn TEST_ENTITY_URN =
      UrnUtils.getUrn(
          "urn:li:dataset:(urn:li:dataPlatform:bigquery,calm-pagoda-323403.jaffle_shop.orders,PROD)");
  private static final Urn TEST_USER_A = UrnUtils.getUrn("urn:li:corpUser:a");
  private static final Urn TEST_USER_B = UrnUtils.getUrn("urn:li:corpUser:b");
  private static final Urn TEST_GROUP_A = UrnUtils.getUrn("urn:li:corpGroup:a");
  private static final Urn TEST_GROUP_B = UrnUtils.getUrn("urn:li:corpGroup:b");
  private static final Urn TECH_OWNER =
      UrnUtils.getUrn("urn:li:ownershipType:__system__technical_owner");
  private static final Urn BUS_OWNER =
      UrnUtils.getUrn("urn:li:ownershipType:__system__business_owner");

  @Test
  public void ownershipTypeMutationNoneType() {
    OwnerTypeMap testHook = new OwnerTypeMap(ASPECT_PLUGIN_CONFIG);
    Ownership ownership = buildOwnership(Map.of(TEST_USER_A, List.of(), TEST_GROUP_A, List.of()));
    testHook.writeMutation(buildMCP(null, ownership), ASPECT_RETRIEVER);

    assertEquals(
        ownership.getOwnerTypes(),
        new UrnArrayMap(
            Map.of(
                DEFAULT_OWNERSHIP_TYPE_URN.toString(),
                new UrnArray(List.of(TEST_USER_A, TEST_GROUP_A)))),
        "Expected generic owners to be grouped by `none` ownership type.");
  }

  @Test
  public void ownershipTypeMutationNoneTypeAdd() {
    OwnerTypeMap testHook = new OwnerTypeMap(ASPECT_PLUGIN_CONFIG);
    Ownership oldOwnership = buildOwnership(Map.of(TEST_USER_A, List.of()));
    Ownership newOwnership =
        buildOwnership(Map.of(TEST_USER_A, List.of(), TEST_GROUP_A, List.of()));
    testHook.writeMutation(buildMCP(oldOwnership, newOwnership), ASPECT_RETRIEVER);

    assertEquals(
        newOwnership.getOwnerTypes(),
        new UrnArrayMap(
            Map.of(
                DEFAULT_OWNERSHIP_TYPE_URN.toString(),
                new UrnArray(List.of(TEST_USER_A, TEST_GROUP_A)))),
        "Expected generic owners to be grouped by `none` ownership type.");
  }

  @Test
  public void ownershipTypeMutationNoneTypeRemove() {
    OwnerTypeMap testHook = new OwnerTypeMap(ASPECT_PLUGIN_CONFIG);
    Ownership oldOwnership =
        buildOwnership(Map.of(TEST_USER_A, List.of(), TEST_GROUP_A, List.of()));
    Ownership newOwnership = buildOwnership(Map.of(TEST_USER_A, List.of()));
    testHook.writeMutation(buildMCP(oldOwnership, newOwnership), ASPECT_RETRIEVER);

    assertEquals(
        newOwnership.getOwnerTypes(),
        new UrnArrayMap(
            Map.of(DEFAULT_OWNERSHIP_TYPE_URN.toString(), new UrnArray(List.of(TEST_USER_A)))),
        "Expected generic owners to be grouped by `none` ownership type.");
  }

  @Test
  public void ownershipTypeMutationMixedType() {
    OwnerTypeMap testHook = new OwnerTypeMap(ASPECT_PLUGIN_CONFIG);
    Ownership ownership =
        buildOwnership(
            Map.of(
                TEST_USER_A,
                List.of(),
                TEST_GROUP_A,
                List.of(),
                TEST_USER_B,
                List.of(BUS_OWNER),
                TEST_GROUP_B,
                List.of(TECH_OWNER)));
    testHook.writeMutation(buildMCP(null, ownership), ASPECT_RETRIEVER);

    assertEquals(
        ownership.getOwnerTypes(),
        new UrnArrayMap(
            Map.of(
                DEFAULT_OWNERSHIP_TYPE_URN.toString(),
                new UrnArray(List.of(TEST_USER_A, TEST_GROUP_A)),
                BUS_OWNER.toString(),
                new UrnArray(List.of(TEST_USER_B)),
                TECH_OWNER.toString(),
                new UrnArray(List.of(TEST_GROUP_B)))),
        "Expected generic owners to be grouped by `none` ownership type as well as specified types.");
  }

  @Test
  public void ownershipTypeMutationMixedTypeAdd() {
    OwnerTypeMap testHook = new OwnerTypeMap(ASPECT_PLUGIN_CONFIG);
    Ownership oldOwnership =
        buildOwnership(Map.of(TEST_USER_A, List.of(), TEST_USER_B, List.of(BUS_OWNER)));
    Ownership newOwnership =
        buildOwnership(
            Map.of(
                TEST_USER_A,
                List.of(),
                TEST_GROUP_A,
                List.of(),
                TEST_USER_B,
                List.of(BUS_OWNER),
                TEST_GROUP_B,
                List.of(TECH_OWNER)));
    testHook.writeMutation(buildMCP(oldOwnership, newOwnership), ASPECT_RETRIEVER);

    assertEquals(
        newOwnership.getOwnerTypes(),
        new UrnArrayMap(
            Map.of(
                DEFAULT_OWNERSHIP_TYPE_URN.toString(),
                new UrnArray(List.of(TEST_USER_A, TEST_GROUP_A)),
                BUS_OWNER.toString(),
                new UrnArray(List.of(TEST_USER_B)),
                TECH_OWNER.toString(),
                new UrnArray(List.of(TEST_GROUP_B)))),
        "Expected generic owners to be grouped by `none` ownership type as well as specified types.");
  }

  @Test
  public void ownershipTypeMutationMixedTypeRemove() {
    OwnerTypeMap testHook = new OwnerTypeMap(ASPECT_PLUGIN_CONFIG);
    Ownership oldOwnership =
        buildOwnership(
            Map.of(
                TEST_USER_A,
                List.of(),
                TEST_GROUP_A,
                List.of(),
                TEST_USER_B,
                List.of(BUS_OWNER),
                TEST_GROUP_B,
                List.of(TECH_OWNER)));
    Ownership newOwnership =
        buildOwnership(Map.of(TEST_GROUP_A, List.of(), TEST_GROUP_B, List.of(TECH_OWNER)));
    testHook.writeMutation(buildMCP(oldOwnership, newOwnership), ASPECT_RETRIEVER);

    assertEquals(
        newOwnership.getOwnerTypes(),
        new UrnArrayMap(
            Map.of(
                DEFAULT_OWNERSHIP_TYPE_URN.toString(),
                new UrnArray(List.of(TEST_GROUP_A)),
                BUS_OWNER.toString(),
                new UrnArray(),
                TECH_OWNER.toString(),
                new UrnArray(List.of(TEST_GROUP_B)))),
        "Expected generic owners to be grouped by `none` ownership type as well as specified types.");
  }

  private static Ownership buildOwnership(Map<Urn, List<Urn>> ownershipTypes) {
    Ownership ownership = new Ownership();
    ownership.setOwners(
        ownershipTypes.entrySet().stream()
            .flatMap(
                entry -> {
                  if (entry.getValue().isEmpty()) {
                    Owner owner = new Owner();
                    owner.setOwner(entry.getKey());
                    return Stream.of(owner);
                  } else {
                    return entry.getValue().stream()
                        .map(
                            typeUrn -> {
                              Owner owner = new Owner();
                              owner.setOwner(entry.getKey());
                              owner.setTypeUrn(typeUrn);
                              return owner;
                            });
                  }
                })
            .collect(Collectors.toCollection(OwnerArray::new)));
    return ownership;
  }

  private static Set<ChangeMCP> buildMCP(@Nullable Ownership oldOwnership, Ownership newOwnership) {
    return TestMCP.ofOneMCP(TEST_ENTITY_URN, oldOwnership, newOwnership, ENTITY_REGISTRY);
  }
}
