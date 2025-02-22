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
import com.linkedin.dataset.ViewProperties;
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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.*;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.*;
import org.mockito.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ViewOpsDelegateTest {
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

  private ViewOpsDelegate viewDelegate;

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

    viewDelegate =
        new ViewOpsDelegate(
            mockWarehouse, identifier, mockEntityService, mockOperationContext, mockFileIOFactory) {
          @Override
          IcebergBatch newIcebergBatch(OperationContext operationContext) {
            return mockIcebergBatch;
          }

          @Override
          protected DatasetProfile getDataSetProfile(ViewMetadata metadata) {
            return stubDatasetProfile;
          }
        };

    ActorContext actorContext = mock(ActorContext.class);
    when(mockOperationContext.getActorContext()).thenReturn(actorContext);
    when(actorContext.getActorUrn()).thenReturn(new CorpuserUrn("urn:li:corpuser:testUser"));
  }

  private ViewProperties mockSqlRepresentation(ViewMetadata viewMetadata) {
    String dialect = "someDialect";
    String sql = "select * from something";

    SQLViewRepresentation sqlViewRepresentation = mock(SQLViewRepresentation.class);
    ViewVersion viewVersion = mock(ViewVersion.class);

    when(sqlViewRepresentation.dialect()).thenReturn(dialect);
    when(sqlViewRepresentation.sql()).thenReturn(sql);

    when(viewVersion.representations()).thenReturn(List.of(sqlViewRepresentation));
    when(viewMetadata.currentVersion()).thenReturn(viewVersion);

    return new ViewProperties().setViewLogic(sql).setMaterialized(false).setViewLanguage(dialect);
  }

  // CREATION
  @Test
  public void testCreateViewSuccess() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    ViewMetadata metadata = mock(ViewMetadata.class);
    when(metadata.schema()).thenReturn(schema);
    when(metadata.location()).thenReturn("s3://bucket/table");

    ViewProperties viewProperties = mockSqlRepresentation(metadata);

    // Simulating new table creation
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(null);

    DatasetUrn datasetUrn = mock(DatasetUrn.class);

    when(mockWarehouse.createDataset(eq(identifier), eq(true), same(mockIcebergBatch)))
        .thenReturn(datasetUrn);
    IcebergBatch.EntityBatch entityBatch = mock(IcebergBatch.EntityBatch.class);
    when(mockIcebergBatch.createEntity(
            eq(datasetUrn),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_ICEBERG_METADATA_ASPECT_NAME),
            eq(
                new IcebergCatalogInfo()
                    .setMetadataPointer("s3://bucket/metadata/00001-metadata.json")
                    .setView(true))))
        .thenReturn(entityBatch);

    viewDelegate.doCommit(
        null, new MetadataWrapper<>(metadata), () -> "s3://bucket/metadata/00001-metadata.json");

    verify(mockWarehouse).createDataset(eq(identifier), eq(true), same(mockIcebergBatch));
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
            eq(SUB_TYPES_ASPECT_NAME), eq(new SubTypes().setTypeNames(new StringArray("View"))));
    verify(entityBatch).platformInstance(eq(catalogName));
    verify(entityBatch).aspect(eq(VIEW_PROPERTIES_ASPECT_NAME), eq(viewProperties));

    verifyDatasetProfile();

    verifyNoMoreInteractions(entityBatch);
  }

  @Test(
      expectedExceptions = AlreadyExistsException.class,
      expectedExceptionsMessageRegExp = "View already exists: " + fullName)
  public void testCreateViewAlreadyExistsFailure() {
    mockWarehouseIcebergMetadata("someLocation", true, "version1");
    ViewMetadata newMetadata = mock(ViewMetadata.class);

    viewDelegate.doCommit(null, new MetadataWrapper<>(newMetadata), null);
  }

  @Test(
      expectedExceptions = AlreadyExistsException.class,
      expectedExceptionsMessageRegExp = "View already exists: " + fullName)
  public void testCreateViewConcurrencyFailure() {
    mockWarehouseIcebergMetadata("someLocation", true, "version1");
    IcebergBatch icebergBatch = mock(IcebergBatch.class);
    when(mockWarehouse.createDataset(eq(identifier), eq(true), same(icebergBatch)))
        .thenThrow(ValidationException.class);

    viewDelegate.doCommit(null, new MetadataWrapper<>(mock(ViewMetadata.class)), null);
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
  public void testUpdateViewSuccess() {
    String existingLocation = "s3://bucket/metadata/00001-metadata.json";
    String existingVersion = "version1";
    int existingSchemaId = 1;

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));
    ViewMetadata metadata = mock(ViewMetadata.class);
    when(metadata.schema()).thenReturn(schema);
    when(metadata.location()).thenReturn("s3://bucket/table");
    when(metadata.currentSchemaId()).thenReturn(existingSchemaId + 1);

    ViewProperties viewProperties = mockSqlRepresentation(metadata);

    ViewMetadata base = mock(ViewMetadata.class);
    when(base.metadataFileLocation()).thenReturn(existingLocation);
    when(base.currentSchemaId()).thenReturn(existingSchemaId);

    Pair<EnvelopedAspect, DatasetUrn> existingDatasetAspect =
        mockWarehouseIcebergMetadata(existingLocation, true, existingVersion);
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(existingDatasetAspect);

    DatasetUrn datasetUrn = existingDatasetAspect.getSecond();

    String newMetadataPointerLocation = "s3://bucket/metadata/00002-metadata.json";
    IcebergCatalogInfo newgCatalogInfo =
        new IcebergCatalogInfo().setMetadataPointer(newMetadataPointerLocation).setView(true);

    IcebergBatch.EntityBatch entityBatch = mock(IcebergBatch.EntityBatch.class);
    when(mockIcebergBatch.conditionalUpdateEntity(
            eq(datasetUrn),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_ICEBERG_METADATA_ASPECT_NAME),
            eq(newgCatalogInfo),
            eq(existingVersion)))
        .thenReturn(entityBatch);

    viewDelegate.doCommit(
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

    verify(entityBatch).aspect(eq(VIEW_PROPERTIES_ASPECT_NAME), eq(viewProperties));
    verifyNoMoreInteractions(entityBatch);

    verifyDatasetProfile();
  }

  @Test(
      expectedExceptions = CommitFailedException.class,
      expectedExceptionsMessageRegExp = "Cannot commit to view " + fullName + ": stale metadata")
  public void testUpdateViewConcurrencyFailure() {
    String existingLocation = "s3://bucket/metadata/00001-metadata.json";
    String existingVersion = "version1";

    String newMetadataPointerLocation = "s3://bucket/metadata/00002-metadata.json";

    ViewMetadata base = mock(ViewMetadata.class);
    when(base.metadataFileLocation()).thenReturn(existingLocation);

    Pair<EnvelopedAspect, DatasetUrn> existingDatasetAspect =
        mockWarehouseIcebergMetadata(existingLocation, true, existingVersion);
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(existingDatasetAspect);

    when(mockEntityService.ingestProposal(
            same(mockOperationContext), same(mockAspectsBatch), eq(false)))
        .thenThrow(ValidationException.class);

    ViewMetadata metadata = mock(ViewMetadata.class);
    when(metadata.location()).thenReturn("s3://bucket/table");

    IcebergCatalogInfo newCatalogInfo =
        new IcebergCatalogInfo().setMetadataPointer(newMetadataPointerLocation).setView(true);

    IcebergBatch.EntityBatch entityBatch = mock(IcebergBatch.EntityBatch.class);
    when(mockIcebergBatch.conditionalUpdateEntity(
            eq(existingDatasetAspect.getSecond()),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_ICEBERG_METADATA_ASPECT_NAME),
            eq(newCatalogInfo),
            eq(existingVersion)))
        .thenReturn(entityBatch);

    ViewProperties viewProperties = mockSqlRepresentation(metadata);
    viewDelegate.doCommit(
        new MetadataWrapper<>(base),
        new MetadataWrapper<>(metadata),
        () -> newMetadataPointerLocation);

    verify(mockIcebergBatch)
        .conditionalUpdateEntity(
            eq(existingDatasetAspect.getSecond()),
            eq(DATASET_ENTITY_NAME),
            eq(DATASET_ICEBERG_METADATA_ASPECT_NAME),
            eq(newCatalogInfo),
            eq(existingVersion));

    verify(entityBatch).aspect(eq(VIEW_PROPERTIES_ASPECT_NAME), eq(viewProperties));
    verifyNoMoreInteractions(entityBatch);
  }

  @Test
  public void testUpdateViewStaleMetadataFailure() {
    String existingLocation = "s3://bucket/metadata/00002-metadata.json";
    String staleLocationBeingUpdated = "s3://bucket/metadata/00001-metadata.json";

    ViewMetadata base = mock(ViewMetadata.class);
    when(base.metadataFileLocation()).thenReturn(staleLocationBeingUpdated);

    Pair<EnvelopedAspect, DatasetUrn> existingDatasetAspect =
        mockWarehouseIcebergMetadata(existingLocation, true, "version1");
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(existingDatasetAspect);

    // new metadata location, metadataWriter etc. should not be accessed in this case
    try {
      viewDelegate.doCommit(
          new MetadataWrapper<>(base), new MetadataWrapper<>(mock(ViewMetadata.class)), null);
      fail();
    } catch (CommitFailedException e) {
      assertEquals(e.getMessage(), "Cannot commit to view " + fullName + ": stale metadata");
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
      expectedExceptions = NoSuchViewException.class,
      expectedExceptionsMessageRegExp = "No such view " + fullName)
  public void testUpdateViewButIsTableFailure() {
    mockWarehouseIcebergMetadata("someLocation", false, "version1");

    viewDelegate.doCommit(
        new MetadataWrapper<>(mock(ViewMetadata.class)),
        new MetadataWrapper<>(mock(ViewMetadata.class)),
        null);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testMissingIcebergMetadata() {
    when(mockWarehouse.getIcebergMetadataEnveloped(identifier)).thenReturn(null);
    viewDelegate.doCommit(
        new MetadataWrapper<>(mock(ViewMetadata.class)),
        new MetadataWrapper<>(mock(ViewMetadata.class)),
        null);
  }

  // REFRESH
  @Test
  public void testViewRefreshSuccessful() {
    String location = "s3://bucket/metadata/00001-metadata.json";
    IcebergCatalogInfo metadata =
        new IcebergCatalogInfo().setMetadataPointer(location).setView(true);
    when(mockWarehouse.getIcebergMetadata(identifier)).thenReturn(Optional.of(metadata));
    InputFile inputFile = mock(InputFile.class);
    when(mockFileIO.newInputFile(location)).thenReturn(inputFile);

    ViewMetadata expectedMetadata = mock(ViewMetadata.class);

    try (MockedStatic<ViewMetadataParser> viewMetadataParserMock =
        Mockito.mockStatic(ViewMetadataParser.class)) {
      viewMetadataParserMock
          .when(() -> ViewMetadataParser.read(same(inputFile)))
          .thenReturn(expectedMetadata);
      ViewMetadata actualMetadata = viewDelegate.refresh();
      assertSame(actualMetadata, expectedMetadata);
    }
  }

  @Test
  public void testRefreshNotFound() {
    when(mockWarehouse.getIcebergMetadata(identifier)).thenReturn(Optional.empty());
    assertNull(viewDelegate.refresh());
  }
}
