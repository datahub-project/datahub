package io.datahubproject.iceberg.catalog.rest.secure;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/iceberg")
public class IcebergConfigApiController extends AbstractIcebergController {
  @GetMapping(value = "/v1/config", produces = MediaType.APPLICATION_JSON_VALUE)
  public ConfigResponse getConfig(
      HttpServletRequest request,
      @RequestParam(value = "warehouse", required = true) String warehouse) {
    log.info("GET CONFIG for warehouse {}", warehouse);

    // check that warehouse exists
    warehouse(warehouse, opContext(request));
    ConfigResponse response = ConfigResponse.builder().withOverride("prefix", warehouse).build();
    log.info("GET CONFIG response: {}", response);
    return response;
  }
}
