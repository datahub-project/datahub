package io.datahubproject.metadata.context.usage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;
import org.testng.annotations.Test;

public class UsageOperationTest {

  @Test
  public void testKeyAndFromKeyRoundTrip() {
    for (UsageOperation operation : UsageOperation.values()) {
      assertEquals(UsageOperation.fromKey(operation.key()), operation);
    }
  }

  @Test
  public void testFromKeyUnknownThrows() {
    assertThrows(IllegalArgumentException.class, () -> UsageOperation.fromKey("not_a_key"));
  }

  @Test
  public void testDefaultGraphqlOperationKindMutations() {
    assertEquals(
        UsageOperation.METADATA_WRITE.defaultGraphqlOperationKind(), GraphQLOperationKind.MUTATION);
    assertEquals(
        UsageOperation.METADATA_INGEST.defaultGraphqlOperationKind(),
        GraphQLOperationKind.MUTATION);
    assertEquals(
        UsageOperation.ENTITY_DELETE.defaultGraphqlOperationKind(), GraphQLOperationKind.MUTATION);
    assertEquals(
        UsageOperation.ASPECT_DELETE.defaultGraphqlOperationKind(), GraphQLOperationKind.MUTATION);
    assertEquals(
        UsageOperation.OTHER_WRITE.defaultGraphqlOperationKind(), GraphQLOperationKind.MUTATION);
    assertEquals(
        UsageOperation.OTHER_OPERATIONS.defaultGraphqlOperationKind(),
        GraphQLOperationKind.MUTATION);
  }

  @Test
  public void testDefaultGraphqlOperationKindQueries() {
    assertEquals(
        UsageOperation.METADATA_READ.defaultGraphqlOperationKind(), GraphQLOperationKind.QUERY);
    assertEquals(
        UsageOperation.SEARCH_QUERY.defaultGraphqlOperationKind(), GraphQLOperationKind.QUERY);
    assertEquals(
        UsageOperation.LINEAGE_QUERY.defaultGraphqlOperationKind(), GraphQLOperationKind.QUERY);
    assertEquals(
        UsageOperation.METADATA_QUERY.defaultGraphqlOperationKind(), GraphQLOperationKind.QUERY);
    assertEquals(
        UsageOperation.OTHER_READ.defaultGraphqlOperationKind(), GraphQLOperationKind.QUERY);
    assertEquals(
        UsageOperation.MCP_QUERY.defaultGraphqlOperationKind(), GraphQLOperationKind.QUERY);
  }
}
