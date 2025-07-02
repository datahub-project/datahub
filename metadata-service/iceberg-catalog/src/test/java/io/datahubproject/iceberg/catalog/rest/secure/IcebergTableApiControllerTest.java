package io.datahubproject.iceberg.catalog.rest.secure;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertSame;

import com.linkedin.common.urn.DatasetUrn;
import io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse;
import io.datahubproject.iceberg.catalog.DataHubRestCatalog;
import io.datahubproject.iceberg.catalog.Utils;
import io.datahubproject.metadata.context.OperationContext;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IcebergTableApiControllerTest
    extends AbstractControllerTest<IcebergTableApiController> {

  private String nsName = "fooNs";
  private Namespace ns = Namespace.of(nsName);
  private String tableName = "fooTable";
  private DatasetUrn datasetUrn = mock(DatasetUrn.class);
  private TableIdentifier tableId = TableIdentifier.of(ns, tableName);
  private DataHubRestCatalog catalog = mock(DataHubRestCatalog.class);

  @Override
  public void onSetup() {
    setupTable();
  }

  @Override
  protected IcebergTableApiController newController() {
    IcebergTableApiController tableApiController =
        new IcebergTableApiController() {
          @Override
          protected DataHubRestCatalog catalog(
              OperationContext operationContext, DataHubIcebergWarehouse warehouse) {
            return catalog;
          }

          @Override
          protected DataHubIcebergWarehouse warehouse(
              String platformInstance, OperationContext operationContext) {
            return warehouse;
          }
        };

    try {
      Field field = IcebergTableApiController.class.getDeclaredField("credentialProvider");
      field.setAccessible(true);
      // reusing mock for tests
      field.set(tableApiController, credentialProvider);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    return tableApiController;
  }

  private void setupTable() {
    when(entityService.exists(
            any(OperationContext.class), eq(Utils.containerUrn(TEST_PLATFORM, ns))))
        .thenReturn(true);
    when(warehouse.getDatasetUrn(eq(tableId))).thenReturn(Optional.of(datasetUrn));
    when(entityService.exists(any(OperationContext.class), same(datasetUrn))).thenReturn(true);
  }

  @Test
  public void testCreateTable() {
    CreateTableRequest createRequest =
        CreateTableRequest.builder()
            .withLocation("s3://someLoc")
            .withName("fooTable")
            .withSchema(new Schema())
            .build();

    LoadTableResponse expectedResponse = mock(LoadTableResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.createTable(same(catalog), eq(ns), same(createRequest)))
          .thenReturn(expectedResponse);
      LoadTableResponse actualResponse =
          controller.createTable(request, TEST_PLATFORM, nsName, createRequest, null);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testCreateTableNamespaceNotExists() {
    controller.createTable(
        request, TEST_PLATFORM, "noSuchNs", mock(CreateTableRequest.class), null);
  }

  @Test
  public void testCreateTableStaged() {
    CreateTableRequest createRequest =
        CreateTableRequest.builder()
            .withLocation("s3://someLoc")
            .withName("fooTable")
            .withSchema(new Schema())
            .stageCreate()
            .build();

    LoadTableResponse expectedResponse = mock(LoadTableResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.stageTableCreate(same(catalog), eq(ns), same(createRequest)))
          .thenReturn(expectedResponse);
      LoadTableResponse actualResponse =
          controller.createTable(request, TEST_PLATFORM, nsName, createRequest, null);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test
  public void testCreateTableVendedCreds() {
    CreateTableRequest createRequest =
        CreateTableRequest.builder()
            .withLocation("s3://someLoc")
            .withName("fooTable")
            .withSchema(new Schema())
            .build();

    TableMetadata metadata = mock(TableMetadata.class);

    LoadTableResponse mockCreateResponse = mock(LoadTableResponse.class);
    when(mockCreateResponse.tableMetadata()).thenReturn(metadata);

    Map<String, String> config = Map.of("someKey", "someValue");
    when(credentialProvider.getCredentials(any(), any())).thenReturn(config);

    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
            Mockito.mockStatic(CatalogHandlers.class);
        MockedStatic<LoadTableResponse> responseStaticMock =
            Mockito.mockStatic(LoadTableResponse.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.createTable(same(catalog), eq(ns), same(createRequest)))
          .thenReturn(mockCreateResponse);
      LoadTableResponse.Builder builder = mock(LoadTableResponse.Builder.class);
      responseStaticMock.when(LoadTableResponse::builder).thenReturn(builder);

      when(builder.withTableMetadata(same(metadata))).thenReturn(builder);
      when(builder.addAllConfig(same(config))).thenReturn(builder);

      LoadTableResponse expectedResponse = mock(LoadTableResponse.class);
      when(builder.build()).thenReturn(expectedResponse);

      LoadTableResponse actualResponse =
          controller.createTable(
              request, TEST_PLATFORM, nsName, createRequest, "vended-credentials");

      assertSame(actualResponse, expectedResponse);
      verify(builder, times(1)).withTableMetadata(same(metadata));
      verify(builder, times(1)).addAllConfig(same(config));
    }
  }

  @Test
  public void testLoadTable() {
    LoadTableResponse expectedResponse = mock(LoadTableResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.loadTable(same(catalog), eq(tableId)))
          .thenReturn(expectedResponse);
      LoadTableResponse actualResponse =
          controller.loadTable(request, TEST_PLATFORM, nsName, tableName, null, null);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test(expectedExceptions = NoSuchTableException.class)
  public void testLoadTableNotExists() {
    controller.loadTable(request, TEST_PLATFORM, nsName, "noSuchTable", null, null);
  }

  @Test
  public void testLoadTableVendedCredentials() {
    TableMetadata metadata = mock(TableMetadata.class);

    LoadTableResponse mockLoadResponse = mock(LoadTableResponse.class);
    when(mockLoadResponse.tableMetadata()).thenReturn(metadata);

    Map<String, String> config = Map.of("someKey", "someValue");
    when(credentialProvider.getCredentials(any(), any())).thenReturn(config);

    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
            Mockito.mockStatic(CatalogHandlers.class);
        MockedStatic<LoadTableResponse> responseStaticMock =
            Mockito.mockStatic(LoadTableResponse.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.loadTable(same(catalog), eq(tableId)))
          .thenReturn(mockLoadResponse);
      LoadTableResponse.Builder builder = mock(LoadTableResponse.Builder.class);
      responseStaticMock.when(LoadTableResponse::builder).thenReturn(builder);

      when(builder.withTableMetadata(same(metadata))).thenReturn(builder);
      when(builder.addAllConfig(same(config))).thenReturn(builder);

      LoadTableResponse expectedResponse = mock(LoadTableResponse.class);
      when(builder.build()).thenReturn(expectedResponse);

      LoadTableResponse actualResponse =
          controller.loadTable(
              request, TEST_PLATFORM, nsName, tableName, "vended-credentials", null);

      assertSame(actualResponse, expectedResponse);
      verify(builder, times(1)).withTableMetadata(same(metadata));
      verify(builder, times(1)).addAllConfig(same(config));
    }
  }

  @Test
  public void testUpdateTable() {
    UpdateTableRequest updateTableRequest = mock(UpdateTableRequest.class);
    LoadTableResponse expectedResponse = mock(LoadTableResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(
              () ->
                  CatalogHandlers.updateTable(same(catalog), eq(tableId), same(updateTableRequest)))
          .thenReturn(expectedResponse);
      LoadTableResponse actualResponse =
          controller.updateTable(request, TEST_PLATFORM, nsName, tableName, updateTableRequest);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test
  public void testDropTable() {
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      controller.dropTable(request, TEST_PLATFORM, nsName, tableName, false);
      catalogHandlersMock.verify(
          () -> CatalogHandlers.dropTable(same(catalog), eq(tableId)), times(1));
    }
  }

  @Test
  public void testPurgeTable() {
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      controller.dropTable(request, TEST_PLATFORM, nsName, tableName, true);
      catalogHandlersMock.verify(
          () -> CatalogHandlers.purgeTable(same(catalog), eq(tableId)), times(1));
    }
  }

  @Test
  public void testRename() {
    RenameTableRequest renameTableRequest =
        RenameTableRequest.builder()
            .withSource(tableId)
            .withDestination(TableIdentifier.of(nsName, "barTable"))
            .build();

    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {

      controller.renameTable(request, TEST_PLATFORM, renameTableRequest);
      catalogHandlersMock.verify(
          () -> CatalogHandlers.renameTable(same(catalog), same(renameTableRequest)), times(1));
    }
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testRenameTargetNamespaceNotExists() {
    RenameTableRequest renameTableRequest =
        RenameTableRequest.builder()
            .withSource(tableId)
            .withDestination(TableIdentifier.of("barNs", "barTable"))
            .build();

    controller.renameTable(request, TEST_PLATFORM, renameTableRequest);
  }

  @Test(
      expectedExceptions = ForbiddenException.class,
      expectedExceptionsMessageRegExp =
          "Data operation MANAGE_TABLES not authorized on fooNs & barNs")
  public void testRenameUnauthorized() {
    setupDefaultAuthorization(false);
    when(entityService.exists(
            any(OperationContext.class),
            eq(Utils.containerUrn(TEST_PLATFORM, Namespace.of("barNs")))))
        .thenReturn(true);
    RenameTableRequest renameTableRequest =
        RenameTableRequest.builder()
            .withSource(tableId)
            .withDestination(TableIdentifier.of("barNs", "barTable"))
            .build();

    controller.renameTable(request, TEST_PLATFORM, renameTableRequest);
  }

  @Test
  public void testRegisterTable() {
    RegisterTableRequest registerTableRequest = mock(RegisterTableRequest.class);
    LoadTableResponse expectedResponse = mock(LoadTableResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(
              () ->
                  CatalogHandlers.registerTable(same(catalog), eq(ns), same(registerTableRequest)))
          .thenReturn(expectedResponse);
      LoadTableResponse actualResponse =
          controller.registerTable(request, TEST_PLATFORM, nsName, registerTableRequest);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test
  public void testListTables() {
    ListTablesResponse expectedResponse = mock(ListTablesResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.listTables(same(catalog), eq(ns)))
          .thenReturn(expectedResponse);
      ListTablesResponse actualResponse =
          controller.listTables(request, TEST_PLATFORM, nsName, null, null);
      assertSame(actualResponse, expectedResponse);
    }
  }
}
