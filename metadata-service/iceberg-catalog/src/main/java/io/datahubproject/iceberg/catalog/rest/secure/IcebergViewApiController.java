package io.datahubproject.iceberg.catalog.rest.secure;

import static io.datahubproject.iceberg.catalog.Utils.*;

import io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse;
import io.datahubproject.iceberg.catalog.DataOperation;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/iceberg")
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
    log.info(
        "CREATE VIEW REQUEST in {}.{}, body {}", platformInstance, namespace, createViewRequest);

    OperationContext operationContext = opContext(request);
    authorize(operationContext, platformInstance, DataOperation.MANAGE_VIEWS, false);

    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    LoadViewResponse createViewResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog -> {
              // ensure namespace exists
              Namespace ns = namespaceFromString(namespace);
              catalog.loadNamespaceMetadata(ns);
              return CatalogHandlers.createView(catalog, ns, createViewRequest);
            });

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
    log.info(
        "UPDATE VIEW REQUEST {}.{}.{}, body {} ",
        platformInstance,
        namespace,
        view,
        updateViewRequest);

    OperationContext operationContext = opContext(request);
    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);
    authorize(
        operationContext,
        warehouse,
        tableIdFromString(namespace, view),
        DataOperation.MANAGE_VIEWS,
        false);

    LoadViewResponse updateViewResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog ->
                CatalogHandlers.updateView(
                    catalog, tableIdFromString(namespace, view), updateViewRequest));

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
    log.info("GET VIEW REQUEST {}.{}.{}", platformInstance, namespace, view);

    OperationContext operationContext = opContext(request);
    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    authorize(
        operationContext,
        warehouse,
        tableIdFromString(namespace, view),
        DataOperation.READ_ONLY,
        false);

    LoadViewResponse getViewResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog -> CatalogHandlers.loadView(catalog, tableIdFromString(namespace, view)));
    log.info("LOAD VIEW RESPONSE {}", getViewResponse);
    return getViewResponse;
  }

  @DeleteMapping(value = "/v1/{prefix}/namespaces/{namespace}/views/{view}")
  public void dropView(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @PathVariable("view") String view) {
    log.info("DROP VIEW REQUEST {}.{}.{}", platformInstance, namespace, view);

    OperationContext operationContext = opContext(request);
    authorize(operationContext, platformInstance, DataOperation.MANAGE_VIEWS, false);
    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    catalogOperation(
        warehouse,
        operationContext,
        catalog -> {
          CatalogHandlers.dropView(catalog, tableIdFromString(namespace, view));
          return null;
        });
    log.info("DROPPED VIEW {}", tableIdFromString(namespace, view));
  }

  @PostMapping(
      value = "/v1/{prefix}/views/rename",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public void renameView(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @RequestBody RenameTableRequest renameTableRequest) {
    log.info(
        "RENAME VIEW REQUEST in platformInstance {}, body {}",
        platformInstance,
        renameTableRequest);

    OperationContext operationContext = opContext(request);
    authorize(operationContext, platformInstance, DataOperation.MANAGE_VIEWS, false);

    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    catalogOperation(
        warehouse,
        operationContext,
        catalog -> {
          CatalogHandlers.renameView(catalog, renameTableRequest);
          return null;
        });

    log.info(
        "RENAMED VIEW {} to {} ", renameTableRequest.source(), renameTableRequest.destination());
  }

  @GetMapping(
      value = "/v1/{prefix}/namespaces/{namespace}/views",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ListTablesResponse listViews(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @RequestParam(value = "pageToken", required = false) String pageToken,
      @RequestParam(value = "pageSize", required = false) Integer pageSize) {
    log.info("LIST VIEWS REQUEST for {}.{}", platformInstance, namespace);

    OperationContext operationContext = opContext(request);
    authorize(operationContext, platformInstance, DataOperation.LIST, false);
    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    ListTablesResponse listTablesResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog -> {
              // ensure namespace exists
              Namespace ns = namespaceFromString(namespace);
              catalog.loadNamespaceMetadata(ns);
              return CatalogHandlers.listViews(catalog, ns);
            });
    log.info("LIST VIEWS RESPONSE {}", listTablesResponse);
    return listTablesResponse;
  }

  @Override
  protected NoSuchViewException noSuchEntityException(
      String platformInstance, TableIdentifier tableIdentifier) {
    return new NoSuchViewException(
        "No such view %s", fullTableName(platformInstance, tableIdentifier));
  }
}
