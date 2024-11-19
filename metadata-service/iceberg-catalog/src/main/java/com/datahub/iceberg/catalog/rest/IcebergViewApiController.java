package com.datahub.iceberg.catalog.rest;

import static com.datahub.iceberg.catalog.Utils.*;

import com.datahub.iceberg.catalog.DataOperation;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
public class IcebergViewApiController extends AbstractIcebergController {

  @PostMapping(
      value = "/v1/{prefix}/namespaces/{namespace}/views",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public LoadViewResponse createView(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @RequestBody CreateViewRequest createViewRequest) {
    log.info("CREATE VIEW REQUEST {}", createViewRequest);

    LoadViewResponse createViewResponse =
        catalogOperation(
            platformInstance,
            request,
            operationContext ->
                authorize(operationContext, platformInstance, DataOperation.MANAGE_VIEWS, false),
            catalog -> {
              // ensure namespace exists
              Namespace ns = namespaceFromString(namespace);
              catalog.loadNamespaceMetadata(ns);
              return CatalogHandlers.createView(catalog, ns, createViewRequest);
            },
            null);

    log.info("CREATE VIEW RESPONSE {}", createViewResponse);
    return createViewResponse;
  }

  @PostMapping(
      value = "/v1/{prefix}/namespaces/{namespace}/views/{view}",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public LoadViewResponse updateView(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @PathVariable("view") String view,
      @RequestBody UpdateTableRequest updateViewRequest) {
    log.info("UPDATE VIEW REQUEST {}.{}, body {} ", namespace, view, updateViewRequest);

    LoadViewResponse updateViewResponse =
        catalogOperation(
            platformInstance,
            request,
            operationContext ->
                authorize(
                    operationContext,
                    platformInstance,
                    tableIdFromString(namespace, view),
                    DataOperation.MANAGE_VIEWS,
                    false),
            catalog ->
                CatalogHandlers.updateView(
                    catalog, tableIdFromString(namespace, view), updateViewRequest),
            null);

    log.info("UPDATE VIEW RESPONSE {}", updateViewResponse);
    return updateViewResponse;
  }

  @GetMapping(
      value = "/v1/{prefix}/namespaces/{namespace}/views/{view}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public LoadViewResponse loadView(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @PathVariable("view") String view) {
    log.info("GET VIEW REQUEST {} {}.{}", platformInstance, namespace, view);

    Namespace ns = RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(view));
    LoadViewResponse getViewResponse =
        catalogOperation(
            platformInstance,
            request,
            operationContext ->
                authorize(
                    operationContext,
                    platformInstance,
                    tableIdFromString(namespace, view),
                    DataOperation.READ_ONLY,
                    false),
            catalog -> CatalogHandlers.loadView(catalog, tableIdFromString(namespace, view)),
            null);
    log.info("LOAD VIEW RESPONSE {}", getViewResponse);
    return getViewResponse;
  }

  @DeleteMapping(value = "/v1/{prefix}/namespaces/{namespace}/views/{view}")
  public void dropView(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @PathVariable("view") String view) {
    log.info("DROP VIEW REQUEST ns {} table {}", namespace, view);
    Namespace ns = RESTUtil.decodeNamespace(URLEncoder.encode(namespace, Charset.defaultCharset()));
    TableIdentifier tableIdentifier = TableIdentifier.of(ns, RESTUtil.decodeString(view));

    catalogOperation(
        platformInstance,
        request,
        operationContext ->
            authorize(operationContext, platformInstance, DataOperation.MANAGE_VIEWS, false),
        catalog -> {
          CatalogHandlers.dropView(catalog, tableIdFromString(namespace, view));
          return null;
        },
        null);
    log.info("DROPPED VIEW {}", tableIdentifier);
  }
}
