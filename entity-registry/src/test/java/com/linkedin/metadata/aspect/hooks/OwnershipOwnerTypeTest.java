package com.linkedin.metadata.aspect.hooks;

import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.common.*;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.test.metadata.aspect.TestEntityRegistry;
import com.linkedin.test.metadata.aspect.batch.TestMCP;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Set;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class OwnershipOwnerTypeTest {

  private EntityRegistry entityRegistry;
  private RetrieverContext mockRetrieverContext;
  private DatasetUrn testDatasetUrn;
  private final OwnershipOwnerTypes test =
      new OwnershipOwnerTypes().setConfig(mock(AspectPluginConfig.class));

  @BeforeTest
  public void init() throws URISyntaxException {
    testDatasetUrn =
        DatasetUrn.createFromUrn(
            UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)"));

    entityRegistry = new TestEntityRegistry();
    AspectRetriever mockAspectRetriever = mock(AspectRetriever.class);
    when(mockAspectRetriever.getEntityRegistry()).thenReturn(entityRegistry);
    GraphRetriever mockGraphRetriever = mock(GraphRetriever.class);
    mockRetrieverContext = mock(RetrieverContext.class);
    when(mockRetrieverContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
    when(mockRetrieverContext.getGraphRetriever()).thenReturn(mockGraphRetriever);
  }

  @Test
  public void testNoChange() throws URISyntaxException {
    Ownership ownership = getOwnership(true, true, false);
    TestMCP mcp = getTestMcpBuilder().recordTemplate(ownership).build();

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(Set.of(mcp), mockRetrieverContext).toList();

    assertEquals(result.size(), 1);
    Pair<ChangeMCP, Boolean> resulted = result.get(0);
    assertEquals(resulted.getSecond(), false);
  }

  @Test
  public void testChangeAddBusinessOwnerType() throws URISyntaxException {
    Ownership ownership = getOwnership(true, false, false);
    TestMCP mcp = getTestMcpBuilder().recordTemplate(ownership).build();

    List<Pair<ChangeMCP, Boolean>> result =
        test.writeMutation(Set.of(mcp), mockRetrieverContext).toList();

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
        test.writeMutation(Set.of(mcp), mockRetrieverContext).toList();

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
      Owner businessOwner =
          new Owner()
              .setOwner(getBusinessOwner())
              .setTypeUrn(new Urn("urn:li:ownershipType:__system__business_owner"));
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
}
