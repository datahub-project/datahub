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
import java.util.Optional;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.*;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IcebergViewApiControllerTest extends AbstractControllerTest<IcebergViewApiController> {

  private String nsName = "fooNs";
  private Namespace ns = Namespace.of(nsName);
  private String viewName = "fooView";
  private DatasetUrn datasetUrn = mock(DatasetUrn.class);
  private TableIdentifier tableId = TableIdentifier.of(ns, viewName);
  private DataHubRestCatalog catalog = mock(DataHubRestCatalog.class);

  @Override
  public void onSetup() {
    setupView();
  }

  @Override
  protected IcebergViewApiController newController() {
    return new IcebergViewApiController() {
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
  }

  private void setupView() {
    when(entityService.exists(
            any(OperationContext.class), eq(Utils.containerUrn(TEST_PLATFORM, ns))))
        .thenReturn(true);
    when(warehouse.getDatasetUrn(eq(tableId))).thenReturn(Optional.of(datasetUrn));
    when(entityService.exists(any(OperationContext.class), same(datasetUrn))).thenReturn(true);
  }

  @Test
  public void testCreateView() {
    CreateViewRequest createRequest = mock(CreateViewRequest.class);

    LoadViewResponse expectedResponse = mock(LoadViewResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.createView(same(catalog), eq(ns), same(createRequest)))
          .thenReturn(expectedResponse);
      LoadViewResponse actualResponse =
          controller.createView(request, TEST_PLATFORM, nsName, createRequest);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testCreateViewNamespaceNotExists() {
    controller.createView(request, TEST_PLATFORM, "noSuchNs", mock(CreateViewRequest.class));
  }

  @Test
  public void testLoadView() {
    LoadViewResponse expectedResponse = mock(LoadViewResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.loadView(same(catalog), eq(tableId)))
          .thenReturn(expectedResponse);
      LoadViewResponse actualResponse =
          controller.loadView(request, TEST_PLATFORM, nsName, viewName);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test(expectedExceptions = NoSuchViewException.class)
  public void testLoadViewNotExists() {
    controller.loadView(request, TEST_PLATFORM, nsName, "noSuchTable");
  }

  @Test
  public void testUpdateView() {
    UpdateTableRequest updateTableRequest = mock(UpdateTableRequest.class);
    LoadViewResponse expectedResponse = mock(LoadViewResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(
              () ->
                  CatalogHandlers.updateView(same(catalog), eq(tableId), same(updateTableRequest)))
          .thenReturn(expectedResponse);
      LoadViewResponse actualResponse =
          controller.updateView(request, TEST_PLATFORM, nsName, viewName, updateTableRequest);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test
  public void testDropView() {
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      controller.dropView(request, TEST_PLATFORM, nsName, viewName);
      catalogHandlersMock.verify(
          () -> CatalogHandlers.dropView(same(catalog), eq(tableId)), times(1));
    }
  }

  @Test
  public void testRename() {
    RenameTableRequest renameTableRequest =
        RenameTableRequest.builder()
            .withSource(tableId)
            .withDestination(TableIdentifier.of(nsName, "barView"))
            .build();

    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {

      controller.renameView(request, TEST_PLATFORM, renameTableRequest);
      catalogHandlersMock.verify(
          () -> CatalogHandlers.renameView(same(catalog), same(renameTableRequest)), times(1));
    }
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testRenameTargetNamespaceNotExists() {
    RenameTableRequest renameTableRequest =
        RenameTableRequest.builder()
            .withSource(tableId)
            .withDestination(TableIdentifier.of("barNs", "barView"))
            .build();

    controller.renameView(request, TEST_PLATFORM, renameTableRequest);
  }

  @Test(
      expectedExceptions = ForbiddenException.class,
      expectedExceptionsMessageRegExp =
          "Data operation MANAGE_VIEWS not authorized on fooNs & barNs")
  public void testRenameUnauthorized() {
    setupDefaultAuthorization(false);
    when(entityService.exists(
            any(OperationContext.class),
            eq(Utils.containerUrn(TEST_PLATFORM, Namespace.of("barNs")))))
        .thenReturn(true);
    RenameTableRequest renameTableRequest =
        RenameTableRequest.builder()
            .withSource(tableId)
            .withDestination(TableIdentifier.of("barNs", "barView"))
            .build();

    controller.renameView(request, TEST_PLATFORM, renameTableRequest);
  }

  @Test
  public void testListViews() {
    ListTablesResponse expectedResponse = mock(ListTablesResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.listViews(same(catalog), eq(ns)))
          .thenReturn(expectedResponse);
      ListTablesResponse actualResponse =
          controller.listViews(request, TEST_PLATFORM, nsName, null, null);
      assertSame(actualResponse, expectedResponse);
    }
  }
}
