/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.iceberg.catalog.rest.secure;

import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/iceberg")
public class IcebergApiController extends AbstractIcebergController {

  @PostMapping(
      value = "/v1/{prefix}/transactions/commit",
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public void commit(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @RequestBody CommitTransactionRequest commitTransactionRequest) {
    log.info("COMMIT REQUEST {} ", commitTransactionRequest);
    throw new UnsupportedOperationException();
  }
}
