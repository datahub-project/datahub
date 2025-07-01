package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.DEFAULT_OWNERSHIP_TYPE_URN;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.*;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OwnershipOwnerTypeTest {

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

  private EntityRegistry entityRegistry;
  private RetrieverContext retrieverContext;
  private DatasetUrn testDatasetUrn;
  private OwnershipOwnerTypes test;

  @BeforeTest
  public void init() throws URISyntaxException {
    testDatasetUrn =
        DatasetUrn.createFromUrn(
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)"));

    entityRegistry = new TestEntityRegistry();
    retrieverContext =
        new RetrieverContext() {
          @Override
          public GraphRetriever getGraphRetriever() {
            return mock(GraphRetriever.class);
          }

          @Override
          public AspectRetriever getAspectRetriever() {
            return mock(AspectRetriever.class);
          }

          @Override
          public SearchRetriever getSearchRetriever() {
            return mock(SearchRetriever.class);
          }
        };

    final AspectPluginConfig aspectPluginConfig =
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

    test = new OwnershipOwnerTypes().setConfig(aspectPluginConfig);
  }

  @Test
  public void testNoChange() throws URISyntaxException {
    Ownership originalOwnership = getOwnership(true, true, false);
    Ownership ownership = getOwnership(true, true, false);
    TestMCP mcp = withPrevious(getTestMcpBuilder().recordTemplate(ownership), ownership).build();

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(Set.of(mcp), retrieverContext).toList();

    assertEquals(result.size(), 1);
    Pair<ChangeMCP, Boolean> resulted = result.get(0);
    assertEquals(
        resulted.getFirst().getAspect(Ownership.class).getOwnerTypes(),
        originalOwnership.getOwnerTypes());
    assertEquals(resulted.getSecond(), false);
  }

  @Test
  public void testChangeAddBusinessOwnerType() throws URISyntaxException {
    Ownership ownership = getOwnership(true, false, false);
    TestMCP mcp = getTestMcpBuilder().recordTemplate(ownership).build();

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(Set.of(mcp), retrieverContext).toList();

    assertEquals(result.size(), 1);
    Pair<ChangeMCP, Boolean> resulted = result.get(0);
    assertEquals(resulted.getSecond(), true);
    Ownership resultOwnership = resulted.getFirst().getAspect(Ownership.class);
    assertEquals(resultOwnership.getOwnerTypes().keySet().size(), 1);
  }

  @Test
  public void testChangeAddBusinessOwnerTypeUrn() throws URISyntaxException {
    Ownership ownership = getOwnership(false, false, true);
    TestMCP mcp = getTestMcpBuilder().recordTemplate(ownership).build();

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(Set.of(mcp), retrieverContext).toList();

    assertEquals(result.size(), 1);
    Pair<ChangeMCP, Boolean> resulted = result.get(0);
    assertEquals(resulted.getSecond(), true);
    Ownership resultOwnership = resulted.getFirst().getAspect(Ownership.class);
    Set<String> ownerTypes = resultOwnership.getOwnerTypes().keySet();
    assertEquals(ownerTypes, Set.of("urn:li:ownershipType:__system__business_owner"));
    assertEquals(
        resultOwnership
            .getOwnerTypes()
            .get("urn:li:ownershipType:__system__business_owner")
            .get(0)
            .toString(),
        "urn:li:corpGroup:business_group");
  }

  private TestMCP.TestMCPBuilder getTestMcpBuilder() {
    return TestMCP.builder()
        .changeType(ChangeType.UPSERT)
        .urn(testDatasetUrn)
        .entitySpec(entityRegistry.getEntitySpec(testDatasetUrn.getEntityType()))
        .aspectSpec(
            entityRegistry
                .getEntitySpec(testDatasetUrn.getEntityType())
                .getAspectSpec(OWNERSHIP_ASPECT_NAME));
  }

  private TestMCP.TestMCPBuilder withPrevious(
      TestMCP.TestMCPBuilder builder, RecordTemplate oldAspect) {
    SystemAspect mockOldSystemAspect = mock(SystemAspect.class);
    when(mockOldSystemAspect.getRecordTemplate()).thenReturn(oldAspect);
    when(mockOldSystemAspect.getAspect(any(Class.class)))
        .thenAnswer(args -> ReadItem.getAspect(args.getArgument(0), oldAspect));
    builder.previousSystemAspect(mockOldSystemAspect);
    return builder;
  }

  private Ownership getOwnership(
      final boolean has_business_owner_with_type,
      final boolean has_business_owner_in_owner_type,
      final boolean has_business_owner_with_type_urn)
      throws URISyntaxException {

    Ownership ownership = new Ownership();
    OwnerArray ownerArray = new OwnerArray();
    if (has_business_owner_with_type) {
      Owner businessOwner =
          new Owner().setOwner(getBusinessOwner()).setType(OwnershipType.BUSINESS_OWNER);
      ownerArray.add(businessOwner);
    }
    if (has_business_owner_with_type_urn) {
      Owner businessOwner = new Owner().setOwner(getBusinessOwner()).setTypeUrn(BUS_OWNER);
      ownerArray.add(businessOwner);
    }

    if (!ownerArray.isEmpty()) {
      ownership.setOwners(ownerArray);
    }
    UrnArrayMap ownerTypes = new UrnArrayMap();
    if (has_business_owner_in_owner_type) {
      ownerTypes.put(
          "urn:li:ownershipType:__system__business_owner", new UrnArray(getBusinessOwner()));
    }
    if (!ownerTypes.isEmpty()) {
      ownership.setOwnerTypes(ownerTypes);
    }
    return ownership;
  }

  private Urn getBusinessOwner() throws URISyntaxException {
    return new Urn("urn:li:corpGroup:business_group");
  }

  /** Test Suite 2 * */
  @Test
  public void ownershipTypeMutationNoneType() {
    Ownership ownership = buildOwnership(Map.of(TEST_USER_A, List.of(), TEST_GROUP_A, List.of()));
    test.writeMutation(buildMCP(null, ownership), retrieverContext);

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
    Ownership oldOwnership = buildOwnership(Map.of(TEST_USER_A, List.of()));
    Ownership newOwnership =
        buildOwnership(Map.of(TEST_USER_A, List.of(), TEST_GROUP_A, List.of()));
    test.writeMutation(buildMCP(oldOwnership, newOwnership), retrieverContext);

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
    Ownership oldOwnership =
        buildOwnership(Map.of(TEST_USER_A, List.of(), TEST_GROUP_A, List.of()));
    Ownership newOwnership = buildOwnership(Map.of(TEST_USER_A, List.of()));
    test.writeMutation(buildMCP(oldOwnership, newOwnership), retrieverContext);

    assertEquals(
        newOwnership.getOwnerTypes(),
        new UrnArrayMap(
            Map.of(DEFAULT_OWNERSHIP_TYPE_URN.toString(), new UrnArray(List.of(TEST_USER_A)))),
        "Expected generic owners to be grouped by `none` ownership type.");
  }

  @Test
  public void ownershipTypeMutationMixedType() {
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
    test.writeMutation(buildMCP(null, ownership), retrieverContext);

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
    test.writeMutation(buildMCP(oldOwnership, newOwnership), retrieverContext);

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
    test.writeMutation(buildMCP(oldOwnership, newOwnership), retrieverContext);

    assertEquals(
        newOwnership.getOwnerTypes(),
        new UrnArrayMap(
            Map.of(
                DEFAULT_OWNERSHIP_TYPE_URN.toString(),
                new UrnArray(List.of(TEST_GROUP_A)),
                TECH_OWNER.toString(),
                new UrnArray(List.of(TEST_GROUP_B)))),
        "Expected generic owners to be grouped by `none` ownership type as well as specified types.");
  }

  @Test
  public void testMutatorWithOwnershipTypeContainingDots() {
    // Create ownership types with dots in their URNs
    Urn ownerTypeWithDots = UrnUtils.getUrn("urn:li:ownershipType:domain.specific.owner");
    Urn anotherTypeWithDots = UrnUtils.getUrn("urn:li:ownershipType:team.lead.technical");
    Urn normalType = UrnUtils.getUrn("urn:li:ownershipType:business_owner");

    // Create test users
    Urn user1 = UrnUtils.getUrn("urn:li:corpUser:john.doe");
    Urn user2 = UrnUtils.getUrn("urn:li:corpUser:jane.smith");
    Urn user3 = UrnUtils.getUrn("urn:li:corpUser:bob");

    // Create ownership with owners having types that contain dots
    Ownership ownership = new Ownership();
    OwnerArray owners = new OwnerArray();

    // User1 with ownership type containing dots
    Owner owner1 = new Owner();
    owner1.setOwner(user1);
    owner1.setTypeUrn(ownerTypeWithDots);
    owners.add(owner1);

    // User2 with another type containing dots
    Owner owner2 = new Owner();
    owner2.setOwner(user2);
    owner2.setTypeUrn(anotherTypeWithDots);
    owners.add(owner2);

    // User3 with normal type (no dots)
    Owner owner3 = new Owner();
    owner3.setOwner(user3);
    owner3.setTypeUrn(normalType);
    owners.add(owner3);

    ownership.setOwners(owners);

    // Execute mutation
    List<Pair<ChangeMCP, Boolean>> results =
        test.writeMutation(buildMCP(null, ownership), retrieverContext)
            .collect(Collectors.toList());

    // Verify results
    assertEquals(results.size(), 1);
    Pair<ChangeMCP, Boolean> result = results.get(0);
    assertTrue(result.getSecond(), "Mutation should have occurred");

    // Verify the ownerTypes map has encoded field names
    UrnArrayMap ownerTypes = ownership.getOwnerTypes();
    assertNotNull(ownerTypes);

    // Check that dots in URN strings are encoded to %2E
    String encodedTypeWithDots = "urn:li:ownershipType:domain%2Especific%2Eowner";
    String encodedAnotherType = "urn:li:ownershipType:team%2Elead%2Etechnical";
    String normalTypeStr = "urn:li:ownershipType:business_owner";

    // Verify the encoded keys exist in the map
    assertTrue(
        ownerTypes.containsKey(encodedTypeWithDots),
        "Map should contain encoded key for: " + ownerTypeWithDots);
    assertTrue(
        ownerTypes.containsKey(encodedAnotherType),
        "Map should contain encoded key for: " + anotherTypeWithDots);
    assertTrue(
        ownerTypes.containsKey(normalTypeStr), "Map should contain normal key for: " + normalType);

    // Verify the values (owner URNs) are correct
    UrnArray dotsTypeOwners = ownerTypes.get(encodedTypeWithDots);
    assertEquals(dotsTypeOwners.size(), 1);
    assertEquals(dotsTypeOwners.get(0), user1);

    UrnArray anotherDotsTypeOwners = ownerTypes.get(encodedAnotherType);
    assertEquals(anotherDotsTypeOwners.size(), 1);
    assertEquals(anotherDotsTypeOwners.get(0), user2);

    UrnArray normalTypeOwners = ownerTypes.get(normalTypeStr);
    assertEquals(normalTypeOwners.size(), 1);
    assertEquals(normalTypeOwners.get(0), user3);
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

  private Set<ChangeMCP> buildMCP(@Nullable Ownership oldOwnership, Ownership newOwnership) {
    return TestMCP.ofOneMCP(TEST_ENTITY_URN, oldOwnership, newOwnership, entityRegistry);
  }
}
