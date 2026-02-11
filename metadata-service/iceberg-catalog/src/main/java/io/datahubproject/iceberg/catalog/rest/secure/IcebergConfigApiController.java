package io.datahubproject.iceberg.catalog.rest.secure;

import jakarta.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/iceberg")
public class IcebergConfigApiController extends AbstractIcebergController {

  @GetMapping(value = "/v1/config", produces = MediaType.APPLICATION_JSON_VALUE)
  public ConfigResponse getConfig(
      HttpServletRequest request, @RequestParam(value = "warehouse") String warehouse) {
    log.info("GET CONFIG for warehouse {}", warehouse);

    // check that warehouse exists
    warehouse(warehouse, opContext(request));

    List<Endpoint> endpoints = buildSupportedEndpoints();

    ConfigResponse response =
        ConfigResponse.builder().withOverride("prefix", warehouse).withEndpoints(endpoints).build();

    log.info("GET CONFIG response: {}", response);
    return response;
  }

  private List<Endpoint> buildSupportedEndpoints() {
    return Arrays.asList(
        // Namespace endpoints
        Endpoint.V1_LIST_NAMESPACES,
        Endpoint.V1_LOAD_NAMESPACE,
        Endpoint.V1_CREATE_NAMESPACE,
        Endpoint.V1_UPDATE_NAMESPACE,
        Endpoint.V1_DELETE_NAMESPACE,
        // Table endpoints
        Endpoint.V1_LIST_TABLES,
        Endpoint.V1_LOAD_TABLE,
        Endpoint.V1_CREATE_TABLE,
        Endpoint.V1_UPDATE_TABLE,
        Endpoint.V1_DELETE_TABLE,
        Endpoint.V1_RENAME_TABLE,
        Endpoint.V1_REGISTER_TABLE,
        // View endpoints
        Endpoint.V1_LIST_VIEWS,
        Endpoint.V1_LOAD_VIEW,
        Endpoint.V1_CREATE_VIEW,
        Endpoint.V1_UPDATE_VIEW,
        Endpoint.V1_DELETE_VIEW,
        Endpoint.V1_RENAME_VIEW);

    // We have the transaction commit endpoint present but just throws unsupported operation
    // exception, hence excluding it.
  }
}
