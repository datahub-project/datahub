package datahub.client.v2.integration;

import static org.junit.Assert.*;

import com.linkedin.common.OwnershipType;
import datahub.client.v2.entity.Container;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Integration tests for Container entity with actual DataHub server.
 *
 * <p>These tests require a running DataHub instance. Set DATAHUB_SERVER environment variable.
 *
 * <p>To run: export DATAHUB_SERVER=http://localhost:8080 ./gradlew
 * :metadata-integration:java:datahub-client:test --tests "*Integration*"
 */
public class ContainerIntegrationTest extends BaseIntegrationTest {

  @Test
  public void testContainerCreateMinimal() throws Exception {
    Container container =
        Container.builder()
            .platform("snowflake")
            .displayName("test_container_minimal_" + System.currentTimeMillis())
            .build();

    client.entities().upsert(container);

    assertNotNull(container.getUrn());
  }

  @Test
  public void testContainerCreateWithMetadata() throws Exception {
    Container container =
        Container.builder()
            .platform("snowflake")
            .displayName("test_container_with_metadata_" + System.currentTimeMillis())
            .description("This is a test container created by Java SDK V2")
            .build();

    client.entities().upsert(container);

    assertNotNull(container.getUrn());
  }

  @Test
  public void testContainerWithTags() throws Exception {
    Container container =
        Container.builder()
            .platform("snowflake")
            .displayName("test_container_with_tags_" + System.currentTimeMillis())
            .build();

    container.addTag("test-tag-1");
    container.addTag("test-tag-2");
    container.addTag("production");

    client.entities().upsert(container);

    assertNotNull(container.getUrn());
  }

  @Test
  public void testContainerWithOwners() throws Exception {
    Container container =
        Container.builder()
            .platform("snowflake")
            .displayName("test_container_with_owners_" + System.currentTimeMillis())
            .build();

    container.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    container.addOwner("urn:li:corpuser:admin", OwnershipType.DATA_STEWARD);

    client.entities().upsert(container);

    assertNotNull(container.getUrn());
  }

  @Test
  public void testContainerWithDomain() throws Exception {
    Container container =
        Container.builder()
            .platform("snowflake")
            .displayName("test_container_with_domain_" + System.currentTimeMillis())
            .build();

    container.setDomain("urn:li:domain:Engineering");

    client.entities().upsert(container);

    assertNotNull(container.getUrn());
  }

  @Test
  public void testContainerWithCustomProperties() throws Exception {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("region", "us-east-1");
    customProps.put("tier", "premium");
    customProps.put("created_by", "java_sdk_v2");

    Container container =
        Container.builder()
            .platform("snowflake")
            .displayName("test_container_with_custom_props_" + System.currentTimeMillis())
            .customProperties(customProps)
            .build();

    client.entities().upsert(container);

    assertNotNull(container.getUrn());
  }

  @Test
  public void testContainerWithParent() throws Exception {
    Container parentContainer =
        Container.builder()
            .platform("snowflake")
            .database("parent_db")
            .displayName("test_parent_container_" + System.currentTimeMillis())
            .build();

    Container childContainer =
        Container.builder()
            .platform("snowflake")
            .database("parent_db")
            .schema("public")
            .displayName("test_child_container_" + System.currentTimeMillis())
            .parentContainer(parentContainer.getContainerUrn())
            .build();

    client.entities().upsert(parentContainer);
    client.entities().upsert(childContainer);

    assertNotNull(parentContainer.getUrn());
    assertNotNull(childContainer.getUrn());
  }

  @Test
  public void testContainerFullMetadata() throws Exception {
    Map<String, String> customProps = new HashMap<>();
    customProps.put("environment", "production");
    customProps.put("compliance_level", "high");

    Container container =
        Container.builder()
            .platform("snowflake")
            .database("analytics_db")
            .displayName("test_container_full_metadata_" + System.currentTimeMillis())
            .description("Complete container with all metadata from Java SDK V2")
            .qualifiedName("prod.snowflake.analytics_db")
            .customProperties(customProps)
            .build();

    // Add all types of metadata
    container.addTag("java-sdk-v2");
    container.addTag("integration-test");
    container.addTag("analytics");

    container.addOwner("urn:li:corpuser:datahub", OwnershipType.TECHNICAL_OWNER);
    container.setDomain("urn:li:domain:Engineering");

    container.addTerm("urn:li:glossaryTerm:Database");

    client.entities().upsert(container);

    assertNotNull(container.getUrn());

    // Validate all metadata was written correctly
    Container fetched = client.entities().get(container.getUrn().toString(), Container.class);
    assertNotNull(fetched);

    // Validate description
    validateEntityDescription(
        container.getUrn().toString(),
        Container.class,
        "Complete container with all metadata from Java SDK V2");

    // Validate tags
    validateEntityHasTags(
        container.getUrn().toString(),
        Container.class,
        java.util.Arrays.asList("java-sdk-v2", "integration-test", "analytics"));

    // Validate owners
    validateEntityHasOwners(
        container.getUrn().toString(),
        Container.class,
        java.util.Arrays.asList("urn:li:corpuser:datahub"));

    // Validate custom properties
    validateEntityCustomProperties(container.getUrn().toString(), Container.class, customProps);
  }

  @Test
  public void testMultipleContainerCreation() throws Exception {
    // Create multiple containers in sequence
    for (int i = 0; i < 5; i++) {
      Container container =
          Container.builder()
              .platform("snowflake")
              .displayName("test_container_multi_" + i + "_" + System.currentTimeMillis())
              .build();

      container.addTag("batch-test");
      container.addTag("container-" + i);

      client.entities().upsert(container);
    }

    // If we get here, all containers were created successfully
    assertTrue(true);
  }
}
