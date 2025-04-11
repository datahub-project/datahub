package io.datahubproject.iceberg.catalog.rest.secure;

import static io.datahubproject.iceberg.catalog.Utils.*;

import io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse;
import io.datahubproject.iceberg.catalog.DataOperation;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.servlet.http.HttpServletRequest;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/iceberg")
public class IcebergNamespaceApiController extends AbstractIcebergController {

  @GetMapping(
      value = "/v1/{prefix}/namespaces/{namespace}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public GetNamespaceResponse getNamespace(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace) {
    log.info("GET NAMESPACE REQUEST {}.{}", platformInstance, namespace);

    OperationContext operationContext = opContext(request);
    // not authorizing get/use namespace operation currently
    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    GetNamespaceResponse getNamespaceResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog -> CatalogHandlers.loadNamespace(catalog, namespaceFromString(namespace)));

    log.info("GET NAMESPACE RESPONSE {}", getNamespaceResponse);
    return getNamespaceResponse;
  }

  @PostMapping(
      value = "/v1/{prefix}/namespaces",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public CreateNamespaceResponse createNamespace(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @RequestBody @Nonnull CreateNamespaceRequest createNamespaceRequest) {
    log.info(
        "CREATE NAMESPACE REQUEST in platformInstance {}, body {}",
        platformInstance,
        createNamespaceRequest);

    OperationContext operationContext = opContext(request);

    authorize(operationContext, platformInstance, DataOperation.MANAGE_NAMESPACES, false);
    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);
    CreateNamespaceResponse createNamespaceResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog -> {
              CatalogHandlers.createNamespace(catalog, createNamespaceRequest);
              return CreateNamespaceResponse.builder()
                  .withNamespace(createNamespaceRequest.namespace())
                  .build();
            });

    log.info("CREATE NAMESPACE RESPONSE {}", createNamespaceResponse);
    return createNamespaceResponse;
  }

  @PostMapping(
      value = "/v1/{prefix}/namespaces/{namespace}",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public UpdateNamespacePropertiesResponse updateNamespace(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @RequestBody @Nonnull UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {
    log.info(
        "UPDATE NAMESPACE REQUEST {}.{}, body {}",
        platformInstance,
        namespace,
        updateNamespacePropertiesRequest);

    OperationContext operationContext = opContext(request);

    authorize(operationContext, platformInstance, DataOperation.MANAGE_NAMESPACES, false);
    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);
    UpdateNamespacePropertiesResponse updateNamespaceResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog ->
                catalog.updateNamespaceProperties(
                    namespaceFromString(namespace), updateNamespacePropertiesRequest));

    log.info("UPDATE NAMESPACE RESPONSE {}", updateNamespaceResponse);
    return updateNamespaceResponse;
  }

  @DeleteMapping(
      value = "/v1/{prefix}/namespaces/{namespace}",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  public void dropNamespace(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace) {
    log.info("DROP NAMESPACE REQUEST {}.{}", platformInstance, namespace);

    OperationContext operationContext = opContext(request);

    authorize(operationContext, platformInstance, DataOperation.MANAGE_NAMESPACES, false);
    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);
    catalogOperation(
        warehouse,
        operationContext,
        catalog -> {
          CatalogHandlers.dropNamespace(catalog, namespaceFromString(namespace));
          return null;
        });

    log.info("DROPPED NAMESPACE {}", namespace);
  }

  @GetMapping(value = "/v1/{prefix}/namespaces", produces = MediaType.APPLICATION_JSON_VALUE)
  public ListNamespacesResponse listNamespaces(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @RequestParam(value = "parent", required = false) String parent,
      @RequestParam(value = "pageToken", required = false) String pageToken,
      @RequestParam(value = "pageSize", required = false) Integer pageSize) {
    log.info("LIST NAMESPACES REQUEST for {}.{}", platformInstance, parent);

    OperationContext operationContext = opContext(request);
    authorize(operationContext, platformInstance, DataOperation.LIST, false);

    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    ListNamespacesResponse listNamespacesResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog -> {
              Namespace ns;
              if (StringUtils.isEmpty(parent)) {
                ns = Namespace.empty();
              } else {
                ns = namespaceFromString(parent);
                // ensure namespace exists
                catalog.loadNamespaceMetadata(ns);
              }
              return CatalogHandlers.listNamespaces(catalog, ns);
            });
    log.info("LIST NAMESPACES RESPONSE {}", listNamespacesResponse);
    return listNamespacesResponse;
  }
}
