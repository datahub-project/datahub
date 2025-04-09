package io.datahubproject.iceberg.catalog;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UtilsTest {

  @Mock private TableMetadata mockTableMetadata;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testPlatformUrn() {
    DataPlatformUrn urn = Utils.platformUrn();
    assertNotNull(urn);
    assertEquals(urn.getPlatformNameEntity(), "iceberg");
    assertEquals(urn.getEntityType(), "dataPlatform");
  }

  @Test
  public void testContainerUrn() {
    String platformInstance = "testInstance";
    Namespace namespace = Namespace.of("db", "schema");

    Urn containerUrn = Utils.containerUrn(platformInstance, namespace);

    assertNotNull(containerUrn);
    assertEquals(containerUrn.toString(), "urn:li:container:iceberg__testInstance.db.schema");

    Namespace namespaceFromUrn =
        Namespace.of(Utils.namespaceNameFromContainerUrn(containerUrn).split("\\."));
    assertEquals(namespaceFromUrn, namespace);
  }

  @Test
  public void testLocations() {
    String mainLocation = "s3://bucket/main";
    String writeDataLocation = "s3://bucket/main/data";
    String writeMetadataLocation = "s3://bucket/main/metadata";

    Map<String, String> properties = mock(Map.class);
    when(properties.containsKey(TableProperties.WRITE_DATA_LOCATION)).thenReturn(true);
    when(properties.containsKey(TableProperties.WRITE_METADATA_LOCATION)).thenReturn(true);
    when(properties.get(TableProperties.WRITE_DATA_LOCATION)).thenReturn(writeDataLocation);
    when(properties.get(TableProperties.WRITE_METADATA_LOCATION)).thenReturn(writeMetadataLocation);

    when(mockTableMetadata.location()).thenReturn(mainLocation);
    when(mockTableMetadata.properties()).thenReturn(properties);

    Set<String> locations = Utils.locations(mockTableMetadata);

    assertEquals(locations.size(), 3);
    assertTrue(locations.contains(mainLocation));
    assertTrue(locations.contains(writeDataLocation));
    assertTrue(locations.contains(writeMetadataLocation));
  }

  public void testEmptyLocations() {
    String mainLocation = "s3://bucket/main";
    String writeDataLocation = "s3://bucket/main/data";
    String writeMetadataLocation = "s3://bucket/main/metadata";

    Map<String, String> properties = mock(Map.class);
    when(properties.containsKey(TableProperties.WRITE_DATA_LOCATION)).thenReturn(false);
    when(properties.containsKey(TableProperties.WRITE_METADATA_LOCATION)).thenReturn(false);
    when(properties.get(TableProperties.WRITE_DATA_LOCATION)).thenReturn(writeDataLocation);
    when(properties.get(TableProperties.WRITE_METADATA_LOCATION)).thenReturn(writeMetadataLocation);

    when(mockTableMetadata.location()).thenReturn(mainLocation);
    when(mockTableMetadata.properties()).thenReturn(properties);

    Set<String> locations = Utils.locations(mockTableMetadata);

    assertEquals(locations.size(), 1);
    assertTrue(locations.contains(mainLocation));
  }

  @Test
  public void testNamespaceFromString() {
    String namespaceStr = "db\u001fschema"; // Note, separator is \u001f
    Namespace namespace = Utils.namespaceFromString(namespaceStr);

    assertNotNull(namespace);
    assertEquals(namespace.levels(), new String[] {"db", "schema"});
  }

  @Test
  public void testTableIdFromString() {
    String namespace = "db\u001fschema";
    String table = "mytable";

    TableIdentifier tableId = Utils.tableIdFromString(namespace, table);

    assertNotNull(tableId);
    assertEquals(tableId.toString(), "db.schema.mytable");
  }

  @Test
  public void testParentDir() {
    String fileLocation = "s3://bucket/path/to/file.txt";
    String parentDir = Utils.parentDir(fileLocation);
    assertEquals(parentDir, "s3://bucket/path/to");
  }
}
