package com.linkedin.metadata.utils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.test.KeyPartEnum;
import com.datahub.test.TestEntityKey;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/** Tests the capabilities of {@link EntityKeyUtils} */
public class EntityKeyUtilsTest {

  private EntityRegistry entityRegistry;

  @BeforeTest
  public void setup() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    entityRegistry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
  }

  @Test
  public void testConvertEntityKeyToUrn() throws Exception {
    final TestEntityKey key = new TestEntityKey();
    key.setKeyPart1("part1");
    key.setKeyPart2(Urn.createFromString("urn:li:testEntity2:part2"));
    key.setKeyPart3(KeyPartEnum.VALUE_1);

    final Urn expectedUrn =
        Urn.createFromString("urn:li:testEntity1:(part1,urn:li:testEntity2:part2,VALUE_1)");
    final Urn actualUrn = EntityKeyUtils.convertEntityKeyToUrn(key, "testEntity1");
    assertEquals(actualUrn.toString(), expectedUrn.toString());
  }

  @Test
  public void testConvertEntityKeyToUrnInternal() throws Exception {
    final Urn urn =
        Urn.createFromString("urn:li:testEntity1:(part1,urn:li:testEntity2:part2,VALUE_1)");
    final TestEntityKey expectedKey = new TestEntityKey();
    expectedKey.setKeyPart1("part1");
    expectedKey.setKeyPart2(Urn.createFromString("urn:li:testEntity2:part2"));
    expectedKey.setKeyPart3(KeyPartEnum.VALUE_1);

    final RecordTemplate actualKey =
        EntityKeyUtils.convertUrnToEntityKeyInternal(urn, expectedKey.schema());
    Assert.assertEquals(actualKey.data(), expectedKey.data());
  }

  @Test
  public void testConvertEntityUrnToKey() throws Exception {
    final Urn urn =
        Urn.createFromString("urn:li:testEntity:(part1,urn:li:testEntity:part2,VALUE_1)");
    final TestEntityKey expectedKey = new TestEntityKey();
    expectedKey.setKeyPart1("part1");
    expectedKey.setKeyPart2(Urn.createFromString("urn:li:testEntity:part2"));
    expectedKey.setKeyPart3(KeyPartEnum.VALUE_1);

    ConfigEntityRegistry entityRegistry =
        new ConfigEntityRegistry(
            TestEntityKey.class.getClassLoader().getResourceAsStream("test-entity-registry.yml"));
    final EntitySpec entitySpec = entityRegistry.getEntitySpec(PegasusUtils.urnToEntityName(urn));

    final RecordTemplate actualKey =
        EntityKeyUtils.convertUrnToEntityKey(urn, entitySpec.getKeyAspectSpec());
    Assert.assertEquals(actualKey.data(), expectedKey.data());
  }

  @Test
  public void testConvertEntityUrnToKeyUrlEncoded() throws URISyntaxException {
    final Urn urn =
        Urn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:s3,urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29,PROD)");
    final AspectSpec keyAspectSpec =
        entityRegistry.getEntitySpec(urn.getEntityType()).getKeyAspectSpec();
    final RecordTemplate actualKey = EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec);

    final DatasetKey expectedKey = new DatasetKey();
    expectedKey.setPlatform(Urn.createFromString("urn:li:dataPlatform:s3"));
    expectedKey.setName(
        "urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29");
    expectedKey.setOrigin(FabricType.PROD);

    assertEquals(actualKey, expectedKey);
  }

  @Test
  public void testGetUrnFromEvent() throws Exception {
    // Create a mock entity registry
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockKeyAspectSpec = mock(AspectSpec.class);

    // Create a test URN
    Urn testUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,testDataset,PROD)");

    // Create a test MetadataChangeLog event
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType("dataset");
    event.setEntityUrn(testUrn);

    // Configure mocks
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getKeyAspectSpec()).thenReturn(mockKeyAspectSpec);

    // Mock the convertUrnToEntityKey method to simulate validation
    RecordTemplate mockKeyRecord = mock(RecordTemplate.class);

    // Use a spy to mock the static method call inside getUrnFromEvent
    try (MockedStatic<EntityKeyUtils> mockedEntityKeyUtils = mockStatic(EntityKeyUtils.class)) {
      mockedEntityKeyUtils
          .when(() -> EntityKeyUtils.convertUrnToEntityKey(eq(testUrn), eq(mockKeyAspectSpec)))
          .thenReturn(mockKeyRecord);

      // Call method under test - redirect to the real method after our mock setup
      mockedEntityKeyUtils
          .when(() -> EntityKeyUtils.getUrnFromLog(eq(event), eq(mockKeyAspectSpec)))
          .thenCallRealMethod();

      // Enable calling the real method under test
      mockedEntityKeyUtils
          .when(() -> EntityKeyUtils.getUrnFromEvent(eq(event), eq(mockEntityRegistry)))
          .thenCallRealMethod();

      // Execute the method
      Urn resultUrn = EntityKeyUtils.getUrnFromEvent(event, mockEntityRegistry);

      // Verify the result
      assertEquals(resultUrn, testUrn);

      // Verify interactions
      verify(mockEntityRegistry).getEntitySpec("dataset");
      verify(mockEntitySpec).getKeyAspectSpec();
      mockedEntityKeyUtils.verify(
          () -> EntityKeyUtils.convertUrnToEntityKey(eq(testUrn), eq(mockKeyAspectSpec)));
    }
  }

  @Test
  public void testGetUrnFromEventWithKeyAspect() throws Exception {
    // Create a mock entity registry
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    EntitySpec mockEntitySpec = mock(EntitySpec.class);
    AspectSpec mockKeyAspectSpec = mock(AspectSpec.class);

    // Create a test DatasetKey
    DatasetKey datasetKey = new DatasetKey();
    datasetKey.setPlatform(Urn.createFromString("urn:li:dataPlatform:hive"));
    datasetKey.setName("testDataset");
    datasetKey.setOrigin(FabricType.PROD);

    // Create test URN that should be generated
    Urn expectedUrn =
        Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,testDataset,PROD)");

    // Create aspect value for the event
    SystemMetadata metadata = new SystemMetadata();
    metadata.setRunId("test-run-id");

    // Create a test MetadataChangeLog event with key aspect instead of URN
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType("dataset");
    event.setEntityKeyAspect(GenericRecordUtils.serializeAspect(datasetKey));

    // Configure mocks
    when(mockEntityRegistry.getEntitySpec("dataset")).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getKeyAspectSpec()).thenReturn(mockKeyAspectSpec);

    // Mock deserializeAspect to return our datasetKey
    try (MockedStatic<GenericRecordUtils> mockedGenericRecordUtils =
        mockStatic(GenericRecordUtils.class)) {
      mockedGenericRecordUtils
          .when(() -> GenericRecordUtils.deserializeAspect(any(), any(), eq(mockKeyAspectSpec)))
          .thenReturn(datasetKey);

      // Mock convertEntityKeyToUrn to return our expected URN
      try (MockedStatic<EntityKeyUtils> mockedEntityKeyUtils =
          mockStatic(EntityKeyUtils.class, CALLS_REAL_METHODS)) {
        mockedEntityKeyUtils
            .when(() -> EntityKeyUtils.convertEntityKeyToUrn(eq(datasetKey), eq("dataset")))
            .thenReturn(expectedUrn);

        // Execute the method
        Urn resultUrn = EntityKeyUtils.getUrnFromEvent(event, mockEntityRegistry);

        // Verify the result
        assertEquals(resultUrn, expectedUrn);

        // Verify interactions
        verify(mockEntityRegistry).getEntitySpec("dataset");
        verify(mockEntitySpec).getKeyAspectSpec();
      }
    }
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = "Failed to get urn from MetadataChangeLog event.*")
  public void testGetUrnFromEventWithInvalidEntityType() {
    // Create a mock entity registry that throws for invalid entity
    EntityRegistry mockEntityRegistry = mock(EntityRegistry.class);
    when(mockEntityRegistry.getEntitySpec("invalidEntity"))
        .thenThrow(new IllegalArgumentException("Entity not found"));

    // Create a test MetadataChangeLog event with invalid entity type
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityType("invalidEntity");

    // Execute the method - should throw exception
    EntityKeyUtils.getUrnFromEvent(event, mockEntityRegistry);
  }
}
