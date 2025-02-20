package io.datahubproject.iceberg.catalog.rest.secure;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse;
import org.apache.iceberg.exceptions.NotFoundException;
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
          .when(() -> DataHubIcebergWarehouse.of(eq(warehouseName), any(), any(), any()))
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
          .when(() -> DataHubIcebergWarehouse.of(eq(warehouseName), any(), any(), any()))
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

  @Override
  protected IcebergConfigApiController newController() {
    return new IcebergConfigApiController();
  }
}
