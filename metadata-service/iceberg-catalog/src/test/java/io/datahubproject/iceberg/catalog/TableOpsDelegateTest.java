package io.datahubproject.iceberg.catalog;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.GenericRecordUtils.serializeAspect;
import static io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse.DATASET_ICEBERG_METADATA_ASPECT_NAME;
import static io.datahubproject.iceberg.catalog.Utils.containerUrn;
import static io.datahubproject.iceberg.catalog.Utils.platformUrn;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.SubTypes;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.container.Container;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.dataset.IcebergCatalogInfo;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.schematron.converters.avro.AvroSchemaConverter;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TableOpsDelegateTest {
  @Mock private DataHubIcebergWarehouse mockWarehouse;
  @Mock private EntityService mockEntityService;
  @Mock private OperationContext mockOperationContext;
  @Mock private FileIOFactory mockFileIOFactory;
  @Mock private FileIO mockFileIO;
  @Mock private IcebergBatch mockIcebergBatch;
  @Mock private AspectsBatch mockAspectsBatch;

  private DatasetProfile stubDatasetProfile;

  private static final TableIdentifier identifier = TableIdentifier.of("db", "entity");
  private static final String catalogName = "fooCatalog";
  private static final String fullName = "fooCatalog.db.entity";

  private TableOpsDelegate tableDelegate;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockFileIOFactory.createIO(any(), any(), any(Set.class))).thenReturn(mockFileIO);
    when(mockFileIOFactory.createIO(any(), any(), any(TableMetadata.class))).thenReturn(mockFileIO);

    when(mockWarehouse.getPlatformInstance()).thenReturn(catalogName);

    AuditStamp batchAuditStamp = new AuditStamp().setTime(Instant.now().toEpochMilli());
    when(mockIcebergBatch.getAuditStamp()).thenReturn(batchAuditStamp);
    when(mockIcebergBatch.asAspectsBatch()).thenReturn(mockAspectsBatch);

    stubDatasetProfile =
        new DatasetProfile()
            .setColumnCount(2)
            .setRowCount(3)
            .setTimestampMillis(batchAuditStamp.getTime());

    tableDelegate =
        new TableOpsDelegate(
            mockWarehouse, identifier, mockEntityService, mockOperationContext, mockFileIOFactory) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }

          @Override
          protected DatasetProfile getDataSetProfile(TableMetadata metadata) {
            return stubDatasetProfile;
          }
        };

    ActorContext actorContext = mock(ActorContext.class);
    when(mockOperationContext.getActorContext()).thenReturn(actorContext);
    when(actorContext.getActorUrn()).thenReturn(new CorpuserUrn("urn:li:corpuser:testUser"));
  }

  // CREATION
  @Test
  public void testCreateTableSuccess() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.schema()).thenReturn(schema);
    when(metadata.location()).thenReturn("s3://bucket/table");

    // Simulating new table creation
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(null);

    DatasetUrn datasetUrn = mock(DatasetUrn.class);

    when(mockWarehouse.createDataset(eq(identifier), eq(false), same(mockIcebergBatch)))
        .thenReturn(datasetUrn);
    IcebergBatch.EntityBatch entityBatch = mock(IcebergBatch.EntityBatch.class);
    when(mockIcebergBatch.createEntity(
            eq(datasetUrn),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_ICEBERG_METADATA_ASPECT_NAME),
            eq(
                new IcebergCatalogInfo()
                    .setMetadataPointer("s3://bucket/metadata/00001-metadata.json")
                    .setView(false))))
        .thenReturn(entityBatch);

    tableDelegate.doCommit(
        null, new MetadataWrapper<>(metadata), () -> "s3://bucket/metadata/00001-metadata.json");

    verify(mockWarehouse).createDataset(eq(identifier), eq(false), same(mockIcebergBatch));
    verify(mockEntityService)
        .ingestProposal(same(mockOperationContext), same(mockAspectsBatch), eq(false));

    // verify schema
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema, fullName);
    AvroSchemaConverter converter = AvroSchemaConverter.builder().build();
    SchemaMetadata schemaMetadata =
        converter.toDataHubSchema(avroSchema, false, false, platformUrn(), null);
    verify(entityBatch).aspect(eq(SCHEMA_METADATA_ASPECT_NAME), eq(schemaMetadata));

    // other aspects populated during creation
    verify(entityBatch)
        .aspect(
            eq(DATASET_PROPERTIES_ASPECT_NAME),
            eq(new DatasetProperties().setName(identifier.name()).setQualifiedName(fullName)));
    verify(entityBatch)
        .aspect(
            eq(CONTAINER_ASPECT_NAME),
            eq(new Container().setContainer(containerUrn(catalogName, Namespace.of("db")))));
    verify(entityBatch)
        .aspect(
            eq(SUB_TYPES_ASPECT_NAME), eq(new SubTypes().setTypeNames(new StringArray("Table"))));
    verify(entityBatch).platformInstance(eq(catalogName));

    verifyDatasetProfile();

    verifyNoMoreInteractions(entityBatch);
    verify(mockIcebergBatch).asAspectsBatch();
  }

  @Test(
      expectedExceptions = AlreadyExistsException.class,
      expectedExceptionsMessageRegExp = "Table already exists: " + fullName)
  public void testCreateTableAlreadyExistsFailure() {
    mockWarehouseIcebergMetadata("someLocation", false, "version1");
    TableMetadata newMetadata = mock(TableMetadata.class);

    tableDelegate.doCommit(null, new MetadataWrapper<>(newMetadata), null);
  }

  @Test(
      expectedExceptions = AlreadyExistsException.class,
      expectedExceptionsMessageRegExp = "Table already exists: " + fullName)
  public void testCreateTableConcurrencyFailure() {
    mockWarehouseIcebergMetadata("someLocation", false, "version1");
    IcebergBatch icebergBatch = mock(IcebergBatch.class);
    when(mockWarehouse.createDataset(eq(identifier), eq(false), same(icebergBatch)))
        .thenThrow(ValidationException.class);

    tableDelegate.doCommit(null, new MetadataWrapper<>(mock(TableMetadata.class)), null);
  }

  private Pair<EnvelopedAspect, DatasetUrn> mockWarehouseIcebergMetadata(
      String metadataPointer, boolean view, String version) {
    IcebergCatalogInfo existingMetadata =
        new IcebergCatalogInfo().setMetadataPointer(metadataPointer).setView(view);

    EnvelopedAspect envelopedAspect =
        new EnvelopedAspect()
            .setValue(new Aspect(existingMetadata.data()))
            .setSystemMetadata(new SystemMetadata().setVersion(version));

    DatasetUrn datasetUrn = mock(DatasetUrn.class);
    Pair<EnvelopedAspect, DatasetUrn> result = new Pair<>(envelopedAspect, datasetUrn);
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(result);

    return result;
  }

  // UPDATES
  @Test
  public void testUpdateTableDataSuccess() {
    String existingLocation = "s3://bucket/metadata/00001-metadata.json";
    String existingVersion = "version1";
    int existingSchemaId = 1;

    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("s3://bucket/table");
    when(metadata.currentSchemaId()).thenReturn(existingSchemaId);

    TableMetadata base = mock(TableMetadata.class);
    when(base.metadataFileLocation()).thenReturn(existingLocation);
    when(base.currentSchemaId()).thenReturn(existingSchemaId);

    Pair<EnvelopedAspect, DatasetUrn> existingDatasetAspect =
        mockWarehouseIcebergMetadata(existingLocation, false, existingVersion);
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(existingDatasetAspect);

    DatasetUrn datasetUrn = existingDatasetAspect.getSecond();

    String newMetadataPointerLocation = "s3://bucket/metadata/00002-metadata.json";
    IcebergCatalogInfo newCatalogInfo =
        new IcebergCatalogInfo().setMetadataPointer(newMetadataPointerLocation).setView(false);

    IcebergBatch.EntityBatch entityBatch = mock(IcebergBatch.EntityBatch.class);
    when(mockIcebergBatch.conditionalUpdateEntity(
            eq(datasetUrn),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_ICEBERG_METADATA_ASPECT_NAME),
            eq(newCatalogInfo),
            eq(existingVersion)))
        .thenReturn(entityBatch);

    tableDelegate.doCommit(
        new MetadataWrapper<>(base),
        new MetadataWrapper<>(metadata),
        () -> newMetadataPointerLocation);

    verify(mockEntityService)
        .ingestProposal(same(mockOperationContext), same(mockAspectsBatch), eq(false));

    verifyNoMoreInteractions(entityBatch);

    verifyDatasetProfile();
  }

  @Test
  public void testUpdateTableSchemaSuccess() {
    String existingLocation = "s3://bucket/metadata/00001-metadata.json";
    String existingVersion = "version1";
    int existingSchemaId = 1;

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.schema()).thenReturn(schema);
    when(metadata.location()).thenReturn("s3://bucket/table");
    when(metadata.currentSchemaId()).thenReturn(existingSchemaId + 1);

    TableMetadata base = mock(TableMetadata.class);
    when(base.metadataFileLocation()).thenReturn(existingLocation);
    when(base.currentSchemaId()).thenReturn(existingSchemaId);

    Pair<EnvelopedAspect, DatasetUrn> existingDatasetAspect =
        mockWarehouseIcebergMetadata(existingLocation, false, existingVersion);
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(existingDatasetAspect);

    DatasetUrn datasetUrn = existingDatasetAspect.getSecond();

    String newMetadataPointerLocation = "s3://bucket/metadata/00002-metadata.json";
    IcebergCatalogInfo newgCatalogInfo =
        new IcebergCatalogInfo().setMetadataPointer(newMetadataPointerLocation).setView(false);

    IcebergBatch.EntityBatch entityBatch = mock(IcebergBatch.EntityBatch.class);
    when(mockIcebergBatch.conditionalUpdateEntity(
            eq(datasetUrn),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_ICEBERG_METADATA_ASPECT_NAME),
            eq(newgCatalogInfo),
            eq(existingVersion)))
        .thenReturn(entityBatch);

    tableDelegate.doCommit(
        new MetadataWrapper<>(base),
        new MetadataWrapper<>(metadata),
        () -> newMetadataPointerLocation);

    verify(mockEntityService)
        .ingestProposal(same(mockOperationContext), same(mockAspectsBatch), eq(false));

    // verify schema
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema, fullName);
    AvroSchemaConverter converter = AvroSchemaConverter.builder().build();
    SchemaMetadata schemaMetadata =
        converter.toDataHubSchema(avroSchema, false, false, platformUrn(), null);
    verify(entityBatch).aspect(eq(SCHEMA_METADATA_ASPECT_NAME), eq(schemaMetadata));

    verifyNoMoreInteractions(entityBatch);

    verifyDatasetProfile();
  }

  @Test
  public void testUpdateTableConcurrencyFailure() {
    String existingLocation = "s3://bucket/metadata/00001-metadata.json";
    String existingVersion = "version1";

    TableMetadata base = mock(TableMetadata.class);
    when(base.metadataFileLocation()).thenReturn(existingLocation);

    Pair<EnvelopedAspect, DatasetUrn> existingDatasetAspect =
        mockWarehouseIcebergMetadata(existingLocation, false, existingVersion);
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(existingDatasetAspect);

    when(mockEntityService.ingestProposal(
            same(mockOperationContext), same(mockAspectsBatch), eq(false)))
        .thenThrow(ValidationException.class);

    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.location()).thenReturn("s3://bucket/table");

    String newMetadataPointerLocation = "s3://bucket/metadata/00002-metadata.json";
    try {
      tableDelegate.doCommit(
          new MetadataWrapper<>(base),
          new MetadataWrapper<>(metadata),
          () -> newMetadataPointerLocation);
      fail();
    } catch (CommitFailedException e) {
      assertEquals(e.getMessage(), "Cannot commit to table " + fullName + ": stale metadata");
    }
    IcebergCatalogInfo newCatalogInfo =
        new IcebergCatalogInfo().setMetadataPointer(newMetadataPointerLocation).setView(false);

    verify(mockIcebergBatch)
        .conditionalUpdateEntity(
            eq(existingDatasetAspect.getSecond()),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_ICEBERG_METADATA_ASPECT_NAME),
            eq(newCatalogInfo),
            eq(existingVersion));
  }

  @Test
  public void testUpdateTableStaleMetadataFailure() {
    String existingLocation = "s3://bucket/metadata/00002-metadata.json";
    String staleLocationBeingUpdated = "s3://bucket/metadata/00001-metadata.json";

    TableMetadata base = mock(TableMetadata.class);
    when(base.metadataFileLocation()).thenReturn(staleLocationBeingUpdated);

    Pair<EnvelopedAspect, DatasetUrn> existingDatasetAspect =
        mockWarehouseIcebergMetadata(existingLocation, false, "version1");
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(existingDatasetAspect);

    // new metadata location, metadataWriter etc. should not be accessed in this case
    try {
      tableDelegate.doCommit(
          new MetadataWrapper<>(base), new MetadataWrapper<>(mock(TableMetadata.class)), null);
      fail();
    } catch (CommitFailedException e) {
      assertEquals(e.getMessage(), "Cannot commit to table " + fullName + ": stale metadata");
    }

    verifyNoInteractions(mockEntityService);
    verifyNoInteractions(mockFileIOFactory);
  }

  private void verifyDatasetProfile() {
    ArgumentCaptor<MetadataChangeProposal> datasetProfileMcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(mockEntityService)
        .ingestProposal(
            same(mockOperationContext), datasetProfileMcpCaptor.capture(), any(), eq(true));
    assertEquals(
        datasetProfileMcpCaptor.getValue().getAspect(), serializeAspect(stubDatasetProfile));
  }

  @Test(
      expectedExceptions = NoSuchTableException.class,
      expectedExceptionsMessageRegExp = "No such table " + fullName)
  public void testUpdateTableButIsViewFailure() {
    mockWarehouseIcebergMetadata("someLocation", true, "version1");

    tableDelegate.doCommit(
        new MetadataWrapper<>(mock(TableMetadata.class)),
        new MetadataWrapper<>(mock(TableMetadata.class)),
        null);
  }

  // REFRESH
  @Test
  public void testTableRefreshSuccessful() {
    String location = "s3://bucket/metadata/00001-metadata.json";
    // Arrange
    IcebergCatalogInfo metadata =
        new IcebergCatalogInfo().setMetadataPointer(location).setView(false);
    when(mockWarehouse.getIcebergMetadata(identifier)).thenReturn(Optional.of(metadata));
    when(mockFileIO.newInputFile(location)).thenReturn(mock(InputFile.class));

    TableMetadata expectedMetadata = mock(TableMetadata.class);

    try (MockedStatic<TableMetadataParser> tableMetadataParserMock =
        Mockito.mockStatic(TableMetadataParser.class)) {
      tableMetadataParserMock
          .when(() -> TableMetadataParser.read(same(mockFileIO), eq(location)))
          .thenReturn(expectedMetadata);
      TableMetadata actualMetadata = tableDelegate.refresh();
      assertSame(actualMetadata, expectedMetadata);
    }
  }

  @Test
  public void testRefreshNotFound() {
    when(mockWarehouse.getIcebergMetadata(identifier)).thenReturn(Optional.empty());
    assertNull(tableDelegate.refresh());
  }
}
