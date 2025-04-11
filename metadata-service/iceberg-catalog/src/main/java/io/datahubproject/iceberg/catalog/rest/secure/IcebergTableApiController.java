package io.datahubproject.iceberg.catalog.rest.secure;

import static io.datahubproject.iceberg.catalog.Utils.*;

import com.linkedin.metadata.authorization.PoliciesConfig;
import io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse;
import io.datahubproject.iceberg.catalog.DataOperation;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.metadata.context.OperationContext;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/iceberg")
public class IcebergTableApiController extends AbstractIcebergController {

  @Autowired private CredentialProvider credentialProvider;

  @PostMapping(
      value = "/v1/{prefix}/namespaces/{namespace}/tables",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public LoadTableResponse createTable(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @RequestBody CreateTableRequest createTableRequest,
      @RequestHeader(value = "X-Iceberg-Access-Delegation") String xIcebergAccessDelegation) {
    log.info(
        "CREATE TABLE REQUEST in {}.{}, body {}", platformInstance, namespace, createTableRequest);

    OperationContext operationContext = opContext(request);
    PoliciesConfig.Privilege privilege =
        authorize(operationContext, platformInstance, DataOperation.MANAGE_TABLES, false);

    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);
    LoadTableResponse createTableResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog -> {
              // ensure namespace exists
              Namespace ns = namespaceFromString(namespace);
              catalog.loadNamespaceMetadata(ns);
              if (createTableRequest.stageCreate()) {
                return CatalogHandlers.stageTableCreate(catalog, ns, createTableRequest);
              } else {
                return CatalogHandlers.createTable(catalog, ns, createTableRequest);
              }
            });
    log.info("CREATE TABLE RESPONSE, excluding creds, {}", createTableResponse);
    return includeCreds(
        platformInstance,
        xIcebergAccessDelegation,
        createTableResponse,
        PoliciesConfig.DATA_READ_WRITE_PRIVILEGE,
        warehouse.getStorageProviderCredentials());
  }

  private LoadTableResponse includeCreds(
      String platformInstance,
      String xIcebergAccessDelegation,
      LoadTableResponse loadTableResponse,
      PoliciesConfig.Privilege privilege,
      CredentialProvider.StorageProviderCredentials storageProviderCredentials) {
    if ("vended-credentials".equals(xIcebergAccessDelegation)) {
      CredentialProvider.CredentialsCacheKey cacheKey =
          new CredentialProvider.CredentialsCacheKey(
              platformInstance, privilege, locations(loadTableResponse.tableMetadata()));
      Map<String, String> creds =
          credentialProvider.getCredentials(cacheKey, storageProviderCredentials);
      /* log.info(
      "STS creds {} for primary table location {}",
      creds,
      loadTableResponse.tableMetadata().location()); */

      return LoadTableResponse.builder()
          .withTableMetadata(loadTableResponse.tableMetadata())
          .addAllConfig(creds)
          .build();
    } else {
      return loadTableResponse;
    }
  }

  @GetMapping(
      value = "/v1/{prefix}/namespaces/{namespace}/tables/{table}",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public LoadTableResponse loadTable(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @PathVariable("table") String table,
      @RequestHeader(value = "X-Iceberg-Access-Delegation", required = false)
          String xIcebergAccessDelegation,
      @RequestParam(value = "snapshots", required = false) String snapshots) {
    log.info(
        "GET TABLE REQUEST {}.{}.{}, access-delegation {}",
        platformInstance,
        namespace,
        table,
        xIcebergAccessDelegation);

    OperationContext operationContext = opContext(request);
    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    PoliciesConfig.Privilege privilege =
        authorize(
            operationContext,
            warehouse,
            tableIdFromString(namespace, table),
            DataOperation.READ_ONLY,
            true);

    LoadTableResponse getTableResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog -> CatalogHandlers.loadTable(catalog, tableIdFromString(namespace, table)));
    log.info("GET TABLE RESPONSE, excluding creds, {}", getTableResponse);

    if (privilege == PoliciesConfig.DATA_MANAGE_TABLES_PRIVILEGE) {
      privilege = PoliciesConfig.DATA_READ_WRITE_PRIVILEGE;
    } else if (privilege == PoliciesConfig.DATA_MANAGE_VIEWS_PRIVILEGE) {
      privilege = PoliciesConfig.DATA_READ_ONLY_PRIVILEGE;
    }
    return includeCreds(
        platformInstance,
        xIcebergAccessDelegation,
        getTableResponse,
        privilege,
        warehouse.getStorageProviderCredentials());
  }

  @PostMapping(
      value = "/v1/{prefix}/namespaces/{namespace}/tables/{table}",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public LoadTableResponse updateTable(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @PathVariable("table") String table,
      @RequestBody UpdateTableRequest updateTableRequest) {

    log.info(
        "UPDATE TABLE REQUEST {}.{}.{}, body {} ",
        platformInstance,
        namespace,
        table,
        updateTableRequest);

    OperationContext operationContext = opContext(request);
    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);
    authorize(
        operationContext,
        warehouse,
        tableIdFromString(namespace, table),
        DataOperation.READ_WRITE,
        false);
    LoadTableResponse updateTableResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog ->
                CatalogHandlers.updateTable(
                    catalog, tableIdFromString(namespace, table), updateTableRequest));

    // not refreshing credentials here.
    log.info("UPDATE TABLE RESPONSE {}", updateTableResponse);

    return updateTableResponse;
  }

  @DeleteMapping(value = "/v1/{prefix}/namespaces/{namespace}/tables/{table}")
  public void dropTable(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @PathVariable("table") String table,
      @RequestParam(value = "purgeRequested", defaultValue = "false") Boolean purgeRequested) {

    log.info(
        "DROP TABLE REQUEST {}.{}.{}, purge = {}",
        platformInstance,
        namespace,
        table,
        purgeRequested);

    OperationContext operationContext = opContext(request);
    authorize(operationContext, platformInstance, DataOperation.MANAGE_TABLES, false);

    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    catalogOperation(
        warehouse,
        operationContext,
        catalog -> {
          TableIdentifier tableIdentifier = tableIdFromString(namespace, table);
          if (purgeRequested) {
            CatalogHandlers.purgeTable(catalog, tableIdentifier);
            log.info("PURGED TABLE {}", tableIdentifier);
          } else {
            CatalogHandlers.dropTable(catalog, tableIdentifier);
            log.info("DROPPED TABLE {}", tableIdentifier);
          }
          return null;
        });
  }

  @PostMapping(
      value = "/v1/{prefix}/tables/rename",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public void renameTable(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @RequestBody RenameTableRequest renameTableRequest) {
    log.info(
        "RENAME TABLE REQUEST in platformInstance {}, body {}",
        platformInstance,
        renameTableRequest);

    OperationContext operationContext = opContext(request);
    authorize(operationContext, platformInstance, DataOperation.MANAGE_TABLES, false);

    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    catalogOperation(
        warehouse,
        operationContext,
        catalog -> {
          CatalogHandlers.renameTable(catalog, renameTableRequest);
          return null;
        });

    log.info(
        "RENAMED TABLE {} to {} ", renameTableRequest.source(), renameTableRequest.destination());
  }

  @PostMapping(
      value = "/v1/{prefix}/namespaces/{namespace}/register",
      produces = MediaType.APPLICATION_JSON_VALUE,
      consumes = MediaType.APPLICATION_JSON_VALUE)
  public LoadTableResponse registerTable(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @RequestBody RegisterTableRequest registerTableRequest) {
    log.info(
        "REGISTER TABLE REQUEST {}.{}, body {}", platformInstance, namespace, registerTableRequest);

    OperationContext operationContext = opContext(request);
    authorize(operationContext, platformInstance, DataOperation.MANAGE_TABLES, false);

    DataHubIcebergWarehouse warehouse = warehouse(platformInstance, operationContext);

    LoadTableResponse registerTableResponse =
        catalogOperation(
            warehouse,
            operationContext,
            catalog -> {
              // ensure namespace exists
              Namespace ns = namespaceFromString(namespace);
              catalog.loadNamespaceMetadata(ns);
              return CatalogHandlers.registerTable(catalog, ns, registerTableRequest);
            });

    log.info("REGISTER TABLE RESPONSE {}", registerTableResponse);
    return registerTableResponse;
  }

  @GetMapping(
      value = "/v1/{prefix}/namespaces/{namespace}/tables",
      produces = MediaType.APPLICATION_JSON_VALUE)
  public ListTablesResponse listTables(
      HttpServletRequest request,
      @PathVariable("prefix") String platformInstance,
      @PathVariable("namespace") String namespace,
      @RequestParam(value = "pageToken", required = false) String pageToken,
      @RequestParam(value = "pageSize", required = false) Integer pageSize) {
    log.info("LIST TABLES REQUEST for {}.{}", platformInstance, namespace);

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
              return CatalogHandlers.listTables(catalog, ns);
            });
    log.info("LIST TABLES RESPONSE {}", listTablesResponse);
    return listTablesResponse;
  }

  @Override
  protected NoSuchTableException noSuchEntityException(
      String platformInstance, TableIdentifier tableIdentifier) {
    return new NoSuchTableException(
        "No such table %s", fullTableName(platformInstance, tableIdentifier));
  }
}
