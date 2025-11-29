package datahub.client.v2.entity;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import com.linkedin.common.urn.Urn;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Tests for Container entity builder and patch-based operations. */
public class ContainerTest {

  // ==================== Builder Tests ====================

  @Test
  public void testContainerBuilderMinimal() {
    Container container =
        Container.builder().platform("snowflake").displayName("My Database").build();

    assertNotNull(container);
    assertNotNull(container.getUrn());
    assertNotNull(container.getContainerUrn());
    assertEquals("container", container.getEntityType());
    assertFalse(
        "Container with displayName has cached aspects, so not new", container.isNewEntity());
  }

  @Test
  public void testContainerBuilderDatabase() {
    Container container =
        Container.builder()
            .platform("postgres")
            .database("analytics_db")
            .displayName("Analytics Database")
            .build();

    assertNotNull(container);
    assertTrue(container.getContainerUrn().contains("urn:li:container:"));
  }

  @Test
  public void testContainerBuilderSchema() {
    Container container =
        Container.builder()
            .platform("snowflake")
            .database("my_db")
            .schema("public")
            .displayName("Public Schema")
            .build();

    assertNotNull(container);
    assertNotNull(container.getContainerUrn());
  }

  @Test
  public void testContainerBuilderWithAllOptions() {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("size_gb", "1000");
    customProps.put("created_by", "admin");

    Container container =
        Container.builder()
            .platform("bigquery")
            .database("my_database")
            .schema("public")
            .env("PROD")
            .platformInstance("my-instance")
            .displayName("My Schema")
            .qualifiedName("prod.bigquery.my_database.public")
            .description("Full container test")
            .externalUrl("https://example.com/container")
            .customProperties(customProps)
            .build();

    assertNotNull(container);
    assertNotNull(container.getDisplayName());
    assertNotNull(container.getQualifiedName());
    assertNotNull(container.getDescription());
    assertNotNull(container.getExternalUrl());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testContainerBuilderMissingPlatform() {
    Container.builder().displayName("Test").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testContainerBuilderMissingDisplayName() {
    Container.builder().platform("postgres").build();
  }

  @Test
  public void testContainerBuilderEnvironment() {
    Container container =
        Container.builder()
            .platform("snowflake")
            .database("test_db")
            .env("DEV")
            .displayName("Test Database")
            .build();

    assertNotNull(container);
    assertTrue(container.getContainerUrn().contains("urn:li:container:"));
  }

  // ==================== Ownership Tests ====================

  @Test
  public void testContainerAddOwner() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);

    List<MetadataChangeProposal> patches = container.getPendingPatches();
    assertFalse("Should have pending patch", patches.isEmpty());
    assertEquals("ownership", patches.get(0).getAspectName());
  }

  @Test
  public void testContainerAddMultipleOwners() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
    container.addOwner("urn:li:corpuser:janedoe", OwnershipType.DATA_STEWARD);

    List<MetadataChangeProposal> patches = container.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testContainerRemoveOwner() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.addOwner("urn:li:corpuser:johndoe", OwnershipType.TECHNICAL_OWNER);
    container.removeOwner("urn:li:corpuser:johndoe");

    List<MetadataChangeProposal> patches = container.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Tag Tests ====================

  @Test
  public void testContainerAddTag() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.addTag("production");
    container.addTag("urn:li:tag:sensitive");

    List<MetadataChangeProposal> patches = container.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("globalTags", patches.get(0).getAspectName());
  }

  @Test
  public void testContainerRemoveTag() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.addTag("tag1");
    container.removeTag("tag1");

    List<MetadataChangeProposal> patches = container.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testContainerTagWithAndWithoutPrefix() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.addTag("production");
    container.addTag("urn:li:tag:development");

    List<MetadataChangeProposal> patches = container.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Glossary Term Tests ====================

  @Test
  public void testContainerAddTerm() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.addTerm("urn:li:glossaryTerm:Database");
    container.addTerm("urn:li:glossaryTerm:ProductionData");

    List<MetadataChangeProposal> patches = container.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
    assertEquals("glossaryTerms", patches.get(0).getAspectName());
  }

  @Test
  public void testContainerRemoveTerm() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.addTerm("urn:li:glossaryTerm:term1");
    container.removeTerm("urn:li:glossaryTerm:term1");

    List<MetadataChangeProposal> patches = container.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  // ==================== Domain Tests ====================

  @Test
  public void testContainerSetDomain() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.setDomain("urn:li:domain:Analytics");

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = container.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testContainerRemoveDomain() throws Exception {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    Urn analyticsDomain = Urn.createFromString("urn:li:domain:Analytics");
    container.setDomain(analyticsDomain.toString());
    container.removeDomain(analyticsDomain);

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = container.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  @Test
  public void testContainerSetNullDomain() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.clearDomains();

    // Verify domain aspect is in cache
    List<MetadataChangeProposalWrapper> mcps = container.toMCPs();
    boolean hasDomainAspect = mcps.stream().anyMatch(mcp -> "domains".equals(mcp.getAspectName()));
    assertTrue("Should have domain aspect in cache", hasDomainAspect);
  }

  // ==================== Description Tests ====================

  @Test
  public void testContainerSetDescription() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.setDescription("Test description");

    List<MetadataChangeProposal> patches = container.getPendingPatches();
    assertFalse("Should have description patch", patches.isEmpty());
    assertEquals("editableContainerProperties", patches.get(0).getAspectName());
  }

  @Test
  public void testContainerBuilderDescription() {
    Container container =
        Container.builder()
            .platform("postgres")
            .displayName("Test Container")
            .description("Builder description")
            .build();

    assertNotNull(container);
    String description = container.getDescription();
    assertNotNull(description);
    assertEquals("Builder description", description);
  }

  // ==================== Property Tests ====================

  @Test
  public void testContainerDisplayName() {
    Container container =
        Container.builder()
            .platform("postgres")
            .displayName("My Test Container")
            .description("Test")
            .build();

    String displayName = container.getDisplayName();
    assertNotNull(displayName);
    assertEquals("My Test Container", displayName);
  }

  @Test
  public void testContainerQualifiedName() {
    Container container =
        Container.builder()
            .platform("postgres")
            .displayName("Test Container")
            .qualifiedName("prod.postgres.my_database")
            .build();

    String qualifiedName = container.getQualifiedName();
    assertNotNull(qualifiedName);
    assertEquals("prod.postgres.my_database", qualifiedName);
  }

  @Test
  public void testContainerExternalUrl() {
    String testUrl = "https://example.com/database/123";
    Container container =
        Container.builder()
            .platform("postgres")
            .displayName("Test Container")
            .externalUrl(testUrl)
            .build();

    String externalUrl = container.getExternalUrl();
    assertNotNull(externalUrl);
    assertEquals(testUrl, externalUrl);
  }

  @Test
  public void testContainerCustomProperties() {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("key1", "value1");
    customProps.put("key2", "value2");

    Container container =
        Container.builder()
            .platform("postgres")
            .displayName("Test Container")
            .customProperties(customProps)
            .build();

    Map<String, String> retrievedProps = container.getCustomProperties();
    assertNotNull(retrievedProps);
    assertTrue(retrievedProps.containsKey("key1"));
    assertTrue(retrievedProps.containsKey("key2"));
  }

  // ==================== Parent Container Tests ====================

  @Test
  public void testContainerSetParentContainer() {
    Container database =
        Container.builder()
            .platform("snowflake")
            .database("my_db")
            .displayName("My Database")
            .build();

    Container schema =
        Container.builder()
            .platform("snowflake")
            .database("my_db")
            .schema("public")
            .displayName("Public Schema")
            .build();

    schema.setContainer(database.getContainerUrn());

    String parentUrn = schema.getParentContainer();
    assertNotNull(parentUrn);
    assertEquals(database.getContainerUrn(), parentUrn);
  }

  @Test
  public void testContainerBuilderParentContainer() {
    Container database =
        Container.builder()
            .platform("snowflake")
            .database("my_db")
            .displayName("My Database")
            .build();

    Container schema =
        Container.builder()
            .platform("snowflake")
            .database("my_db")
            .schema("public")
            .displayName("Public Schema")
            .parentContainer(database.getContainerUrn())
            .build();

    String parentUrn = schema.getParentContainer();
    assertNotNull(parentUrn);
    assertEquals(database.getContainerUrn(), parentUrn);
  }

  @Test
  public void testContainerClearParentContainer() {
    Container database =
        Container.builder()
            .platform("snowflake")
            .database("my_db")
            .displayName("My Database")
            .build();

    Container schema =
        Container.builder()
            .platform("snowflake")
            .database("my_db")
            .schema("public")
            .displayName("Public Schema")
            .parentContainer(database.getContainerUrn())
            .build();

    assertNotNull(schema.getParentContainer());

    schema.clearContainer();

    assertNull(schema.getParentContainer());
  }

  // ==================== Hierarchy Tests ====================

  @Test
  public void testContainerDatabaseSchemaHierarchy() {
    Container database =
        Container.builder()
            .platform("postgres")
            .database("analytics_db")
            .env("PROD")
            .displayName("Analytics Database")
            .build();

    Container schema =
        Container.builder()
            .platform("postgres")
            .database("analytics_db")
            .schema("public")
            .env("PROD")
            .displayName("Public Schema")
            .parentContainer(database.getContainerUrn())
            .build();

    assertNotNull(database.getContainerUrn());
    assertNotNull(schema.getContainerUrn());
    assertNotEquals(database.getContainerUrn(), schema.getContainerUrn());
    assertEquals(database.getContainerUrn(), schema.getParentContainer());
  }

  @Test
  public void testContainerThreeLevelHierarchy() {
    Container database =
        Container.builder().platform("snowflake").database("db1").displayName("Database 1").build();

    Container schema =
        Container.builder()
            .platform("snowflake")
            .database("db1")
            .schema("schema1")
            .displayName("Schema 1")
            .parentContainer(database.getContainerUrn())
            .build();

    Container tableGroup =
        Container.builder()
            .platform("snowflake")
            .database("db1")
            .schema("schema1")
            .displayName("Table Group")
            .qualifiedName("db1.schema1.group1")
            .parentContainer(schema.getContainerUrn())
            .build();

    assertEquals(database.getContainerUrn(), schema.getParentContainer());
    assertEquals(schema.getContainerUrn(), tableGroup.getParentContainer());
  }

  // ==================== Fluent API Tests ====================

  @Test
  public void testContainerFluentAPI() {
    Container container =
        Container.builder()
            .platform("snowflake")
            .database("sales_db")
            .displayName("Sales Database")
            .description("Sales data")
            .build();

    container
        .addTag("production")
        .addTag("critical")
        .addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER)
        .addTerm("urn:li:glossaryTerm:Sales")
        .setDomain("urn:li:domain:Finance");

    List<MetadataChangeProposal> patches = container.getPendingPatches();
    assertFalse("Should have patches", patches.isEmpty());
  }

  @Test
  public void testContainerChainedOperations() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    Container result =
        container
            .addTag("tag1")
            .addTag("tag2")
            .addOwner("urn:li:corpuser:user1", OwnershipType.TECHNICAL_OWNER)
            .setDescription("Description");

    assertSame("Should return same instance", container, result);
  }

  // ==================== URN and Identity Tests ====================

  @Test
  public void testContainerUrnUniqueness() {
    Container container1 =
        Container.builder().platform("postgres").database("db1").displayName("Database 1").build();

    Container container2 =
        Container.builder().platform("postgres").database("db2").displayName("Database 2").build();

    assertNotEquals(
        "Different databases should have different URNs",
        container1.getContainerUrn(),
        container2.getContainerUrn());
  }

  @Test
  public void testContainerUrnDeterministic() {
    Container container1 =
        Container.builder()
            .platform("postgres")
            .database("db1")
            .env("PROD")
            .displayName("Database 1")
            .build();

    Container container2 =
        Container.builder()
            .platform("postgres")
            .database("db1")
            .env("PROD")
            .displayName("Database 1")
            .build();

    assertEquals(
        "Same properties should generate same URN",
        container1.getContainerUrn(),
        container2.getContainerUrn());
  }

  @Test
  public void testContainerSchemaUrnDifferentFromDatabase() {
    Container database =
        Container.builder().platform("postgres").database("db1").displayName("Database 1").build();

    Container schema =
        Container.builder()
            .platform("postgres")
            .database("db1")
            .schema("public")
            .displayName("Public Schema")
            .build();

    assertNotEquals(
        "Database and schema should have different URNs",
        database.getContainerUrn(),
        schema.getContainerUrn());
  }

  // ==================== Utility Tests ====================

  @Test
  public void testContainerToMCPs() {
    Container container =
        Container.builder()
            .platform("postgres")
            .displayName("Test Container")
            .description("Test description")
            .build();

    List<MetadataChangeProposalWrapper> mcps = container.toMCPs();

    assertNotNull(mcps);
    assertFalse(mcps.isEmpty());

    for (MetadataChangeProposalWrapper mcp : mcps) {
      assertEquals("container", mcp.getEntityType());
      assertNotNull(mcp.getEntityUrn());
      assertNotNull(mcp.getAspect());
    }
  }

  @Test
  public void testContainerClearPendingPatches() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    container.addTag("tag1");
    container.addOwner("urn:li:corpuser:owner1", OwnershipType.TECHNICAL_OWNER);

    assertTrue("Should have pending patches", container.hasPendingPatches());

    container.clearPendingPatches();

    assertFalse("Should not have pending patches", container.hasPendingPatches());
  }

  @Test
  public void testContainerEqualsAndHashCode() {
    Container container1 =
        Container.builder().platform("postgres").database("db1").displayName("Database 1").build();

    Container container2 =
        Container.builder().platform("postgres").database("db1").displayName("Database 1").build();

    assertEquals(container1, container2);
    assertEquals(container1.hashCode(), container2.hashCode());
  }

  @Test
  public void testContainerToString() {
    Container container =
        Container.builder().platform("postgres").displayName("Test Container").build();

    String str = container.toString();
    assertNotNull(str);
    assertTrue(str.contains("Container"));
    assertTrue(str.contains("urn"));
  }

  // ==================== Integration Tests ====================

  @Test
  public void testContainerCompleteWorkflow() {
    Container database =
        Container.builder()
            .platform("snowflake")
            .database("analytics")
            .env("PROD")
            .displayName("Analytics Database")
            .description("Production analytics database")
            .qualifiedName("prod.snowflake.analytics")
            .build();

    database
        .addTag("production")
        .addTag("analytics")
        .addOwner("urn:li:corpuser:data_team", OwnershipType.TECHNICAL_OWNER)
        .addTerm("urn:li:glossaryTerm:Database")
        .setDomain("urn:li:domain:Analytics");

    Container schema =
        Container.builder()
            .platform("snowflake")
            .database("analytics")
            .schema("public")
            .env("PROD")
            .displayName("Public Schema")
            .description("Main schema for analytics tables")
            .qualifiedName("prod.snowflake.analytics.public")
            .parentContainer(database.getContainerUrn())
            .build();

    schema
        .addTag("public")
        .addOwner("urn:li:corpuser:analytics_team", OwnershipType.TECHNICAL_OWNER)
        .setDomain("urn:li:domain:Analytics");

    assertNotNull(database.getContainerUrn());
    assertNotNull(schema.getContainerUrn());
    assertEquals(database.getContainerUrn(), schema.getParentContainer());
    assertTrue(database.hasPendingPatches());
    assertTrue(schema.hasPendingPatches());
  }

  @Test
  public void testContainerWithMultipleProperties() {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("size_gb", "5000");
    customProps.put("table_count", "150");
    customProps.put("owner_team", "data_platform");

    Container container =
        Container.builder()
            .platform("bigquery")
            .database("analytics")
            .env("PROD")
            .displayName("Analytics Database")
            .qualifiedName("prod.bigquery.analytics")
            .description("Analytics database with multiple properties")
            .externalUrl("https://console.cloud.google.com/bigquery")
            .customProperties(customProps)
            .build();

    assertNotNull(container.getDisplayName());
    assertNotNull(container.getQualifiedName());
    assertNotNull(container.getDescription());
    assertNotNull(container.getExternalUrl());
    assertNotNull(container.getCustomProperties());
    assertTrue(container.getCustomProperties().size() >= 3);
  }
}
