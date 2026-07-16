package io.datahubproject.iceberg.catalog.rest.secure;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class IcebergConfigApiControllerTest
    extends AbstractControllerTest<IcebergConfigApiController> {

  @Test(expectedExceptions = NotFoundException.class)
  public void testGetConfigNonExistentWarehouse() {
    String warehouseName = "test-warehouse";

    try (MockedStatic<DataHubIcebergWarehouse> warehouseMock =
        Mockito.mockStatic(DataHubIcebergWarehouse.class)) {
      warehouseMock
          .when(() -> DataHubIcebergWarehouse.of(eq(warehouseName), any(), any(), any(), any()))
          .thenThrow(new NotFoundException(""));
      controller.getConfig(request, warehouseName);
    }
  }

  @Test
  public void testGetConfigValidWarehouse() {
    String warehouseName = "test-warehouse";

    try (MockedStatic<DataHubIcebergWarehouse> warehouseMock =
        Mockito.mockStatic(DataHubIcebergWarehouse.class)) {
      warehouseMock
          .when(() -> DataHubIcebergWarehouse.of(eq(warehouseName), any(), any(), any(), any()))
          .thenReturn(null);
      ConfigResponse response = controller.getConfig(request, warehouseName);

      assertNotNull(response, "Config response should not be null");
      assertNotNull(response.overrides(), "Overrides map should not be null");
      assertTrue(
          response.overrides().containsKey("prefix"), "Overrides should contain 'prefix' key");
      assertEquals(
          response.overrides().get("prefix"),
          warehouseName,
          "Warehouse name should match in the config override");
    }
  }

  @Test
  public void testGetConfigReturnsExpectedEndpoints() {
    String warehouseName = "test-warehouse";

    try (MockedStatic<DataHubIcebergWarehouse> warehouseMock =
        Mockito.mockStatic(DataHubIcebergWarehouse.class)) {
      warehouseMock
          .when(() -> DataHubIcebergWarehouse.of(eq(warehouseName), any(), any(), any(), any()))
          .thenReturn(null);
      ConfigResponse response = controller.getConfig(request, warehouseName);

      assertNotNull(response.endpoints(), "Endpoints list should not be null");
      assertFalse(response.endpoints().isEmpty(), "Endpoints list should not be empty");

      // Verify namespace endpoints are present
      assertTrue(
          response.endpoints().contains(Endpoint.V1_LIST_NAMESPACES),
          "Should contain V1_LIST_NAMESPACES endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_LOAD_NAMESPACE),
          "Should contain V1_LOAD_NAMESPACE endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_CREATE_NAMESPACE),
          "Should contain V1_CREATE_NAMESPACE endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_UPDATE_NAMESPACE),
          "Should contain V1_UPDATE_NAMESPACE endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_DELETE_NAMESPACE),
          "Should contain V1_DELETE_NAMESPACE endpoint");

      // Verify table endpoints are present
      assertTrue(
          response.endpoints().contains(Endpoint.V1_LIST_TABLES),
          "Should contain V1_LIST_TABLES endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_LOAD_TABLE),
          "Should contain V1_LOAD_TABLE endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_CREATE_TABLE),
          "Should contain V1_CREATE_TABLE endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_UPDATE_TABLE),
          "Should contain V1_UPDATE_TABLE endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_DELETE_TABLE),
          "Should contain V1_DELETE_TABLE endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_RENAME_TABLE),
          "Should contain V1_RENAME_TABLE endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_REGISTER_TABLE),
          "Should contain V1_REGISTER_TABLE endpoint");

      // Verify view endpoints are present
      assertTrue(
          response.endpoints().contains(Endpoint.V1_LIST_VIEWS),
          "Should contain V1_LIST_VIEWS endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_LOAD_VIEW),
          "Should contain V1_LOAD_VIEW endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_CREATE_VIEW),
          "Should contain V1_CREATE_VIEW endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_UPDATE_VIEW),
          "Should contain V1_UPDATE_VIEW endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_DELETE_VIEW),
          "Should contain V1_DELETE_VIEW endpoint");
      assertTrue(
          response.endpoints().contains(Endpoint.V1_RENAME_VIEW),
          "Should contain V1_RENAME_VIEW endpoint");

      // Verify the total count matches expected endpoints
      assertEquals(response.endpoints().size(), 18, "Should have exactly 18 supported endpoints");
    }
  }

  @Override
  protected IcebergConfigApiController newController() {
    return new IcebergConfigApiController();
  }
}
