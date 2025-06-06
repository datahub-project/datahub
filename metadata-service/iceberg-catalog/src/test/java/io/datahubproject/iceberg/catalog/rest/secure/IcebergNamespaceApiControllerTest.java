package io.datahubproject.iceberg.catalog.rest.secure;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse;
import io.datahubproject.iceberg.catalog.DataHubRestCatalog;
import io.datahubproject.iceberg.catalog.Utils;
import io.datahubproject.metadata.context.OperationContext;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.*;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.*;

public class IcebergNamespaceApiControllerTest
    extends AbstractControllerTest<IcebergNamespaceApiController> {

  private Namespace namespace;
  private String namespaceString;
  private Urn containerUrn;
  private DataHubRestCatalog catalog = mock(DataHubRestCatalog.class);

  @Override
  public void onSetup() {
    setupNamespace();
  }

  @Override
  protected IcebergNamespaceApiController newController() {
    return new IcebergNamespaceApiController() {
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

  private void setupNamespace() {
    Namespace namespaceParent = Namespace.of("db");
    namespace = Namespace.of("db", "schema");
    namespaceString = "db\u001fschema";

    Urn parentContainerUrn = Utils.containerUrn(TEST_PLATFORM, namespaceParent);
    containerUrn = Utils.containerUrn(TEST_PLATFORM, namespace);

    when(entityService.exists(any(OperationContext.class), eq(parentContainerUrn)))
        .thenReturn(true);
  }

  @Test
  public void testCreateNamespace() {
    CreateNamespaceRequest createRequest =
        CreateNamespaceRequest.builder().withNamespace(namespace).build();

    CreateNamespaceResponse expectedResponse = mock(CreateNamespaceResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.createNamespace(same(catalog), same(createRequest)))
          .thenReturn(expectedResponse);
      CreateNamespaceResponse actualResponse =
          controller.createNamespace(request, TEST_PLATFORM, createRequest);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test(expectedExceptions = ForbiddenException.class)
  public void testCreateNamespaceUnauthorized() {
    setupDefaultAuthorization(false);
    CreateNamespaceRequest createRequest =
        CreateNamespaceRequest.builder().withNamespace(namespace).build();

    controller.createNamespace(request, TEST_PLATFORM, createRequest);
  }

  @Test
  public void testGetNamespace() {
    GetNamespaceResponse expectedResponse = mock(GetNamespaceResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.loadNamespace(same(catalog), eq(namespace)))
          .thenReturn(expectedResponse);
      GetNamespaceResponse actualResponse =
          controller.getNamespace(request, TEST_PLATFORM, namespaceString);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testGetNamespaceNonexistent() {
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.loadNamespace(same(catalog), eq(namespace)))
          .thenThrow(new NoSuchNamespaceException(""));
      controller.getNamespace(request, TEST_PLATFORM, namespaceString);
    }
  }

  @Test
  public void testGetNamespaceUnauthorized() {
    setupDefaultAuthorization(false);
    GetNamespaceResponse expectedResponse = mock(GetNamespaceResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.loadNamespace(same(catalog), eq(namespace)))
          .thenReturn(expectedResponse);
      GetNamespaceResponse actualResponse =
          controller.getNamespace(request, TEST_PLATFORM, namespaceString);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test
  public void testUpdateNamespace() {
    UpdateNamespacePropertiesRequest updateRequest = mock(UpdateNamespacePropertiesRequest.class);
    UpdateNamespacePropertiesResponse expectedResponse =
        mock(UpdateNamespacePropertiesResponse.class);
    when(entityService.exists(any(), eq(containerUrn))).thenReturn(true);

    when(catalog.updateNamespaceProperties(eq(namespace), same(updateRequest)))
        .thenReturn(expectedResponse);

    UpdateNamespacePropertiesResponse actualResponse =
        controller.updateNamespace(request, TEST_PLATFORM, namespaceString, updateRequest);
    assertSame(actualResponse, expectedResponse);
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testUpdateNamespaceNonexistent() {
    when(entityService.exists(any(), eq(containerUrn))).thenReturn(false);
    controller.updateNamespace(
        request, TEST_PLATFORM, namespaceString, mock(UpdateNamespacePropertiesRequest.class));
  }

  @Test(expectedExceptions = ForbiddenException.class)
  public void testUpdateNamespaceUnauthorized() {
    setupDefaultAuthorization(false);
    when(entityService.exists(any(), eq(containerUrn))).thenReturn(true);
    controller.updateNamespace(
        request, TEST_PLATFORM, namespaceString, mock(UpdateNamespacePropertiesRequest.class));
  }

  @Test
  public void testDropNamespace() {
    when(entityService.exists(any(), eq(containerUrn))).thenReturn(true);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      controller.dropNamespace(request, TEST_PLATFORM, namespaceString);
      catalogHandlersMock.verify(
          () -> CatalogHandlers.dropNamespace(same(catalog), eq(namespace)), times(1));
    }
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testDropNamespaceNonexistent() {
    when(entityService.exists(any(), eq(containerUrn))).thenReturn(false);
    controller.dropNamespace(request, TEST_PLATFORM, namespaceString);
  }

  @Test(expectedExceptions = ForbiddenException.class)
  public void testDropNamespaceUnauthorized() {
    setupDefaultAuthorization(false);
    when(entityService.exists(any(), eq(containerUrn))).thenReturn(true);
    controller.dropNamespace(request, TEST_PLATFORM, namespaceString);
  }

  @Test
  public void testListNamespacesRoot() {
    ListNamespacesResponse expectedResponse = mock(ListNamespacesResponse.class);

    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.listNamespaces(same(catalog), eq(Namespace.empty())))
          .thenReturn(expectedResponse);
      ListNamespacesResponse actualResponse =
          controller.listNamespaces(request, TEST_PLATFORM, "", null, null);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test
  public void testListNamespaces() {
    when(entityService.exists(any(), eq(containerUrn))).thenReturn(true);
    ListNamespacesResponse expectedResponse = mock(ListNamespacesResponse.class);
    try (MockedStatic<CatalogHandlers> catalogHandlersMock =
        Mockito.mockStatic(CatalogHandlers.class)) {
      catalogHandlersMock
          .when(() -> CatalogHandlers.listNamespaces(same(catalog), eq(namespace)))
          .thenReturn(expectedResponse);
      ListNamespacesResponse actualResponse =
          controller.listNamespaces(request, TEST_PLATFORM, namespaceString, null, null);
      assertSame(actualResponse, expectedResponse);
    }
  }

  @Test(expectedExceptions = NoSuchNamespaceException.class)
  public void testListNamespacesNonexistent() {
    when(entityService.exists(any(), eq(containerUrn))).thenReturn(false);
    controller.listNamespaces(request, TEST_PLATFORM, namespaceString, null, null);
  }

  @Test(expectedExceptions = ForbiddenException.class)
  public void testListNamespacesUnauthorizedRoot() {
    setupDefaultAuthorization(false);
    controller.listNamespaces(request, TEST_PLATFORM, "", null, null);
  }

  @Test(expectedExceptions = ForbiddenException.class)
  public void testListNamespacesUnauthorized() {
    setupDefaultAuthorization(false);
    when(entityService.exists(any(), eq(containerUrn))).thenReturn(true);
    controller.listNamespaces(request, TEST_PLATFORM, namespaceString, null, null);
  }
}
