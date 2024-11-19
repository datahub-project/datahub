package com.datahub.iceberg.catalog.rest;

import static com.datahub.iceberg.catalog.Utils.*;

import com.datahub.iceberg.catalog.CredentialProvider;
import com.datahub.iceberg.catalog.DataOperation;
import com.linkedin.metadata.authorization.PoliciesConfig;
import jakarta.servlet.http.HttpServletRequest;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.CatalogHandlers;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
public class IcebergTableApiController extends AbstractIcebergController {

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
    log.info("CREATE TABLE REQUEST {}", createTableRequest);

    LoadTableResponse createTableResponse =
        catalogOperation(
            platformInstance,
            request,
            operationContext ->
                authorize(operationContext, platformInstance, DataOperation.MANAGE_TABLES, false),
            catalog -> {
              // ensure namespace exists
              Namespace ns = namespaceFromString(namespace);
              catalog.loadNamespaceMetadata(ns);
              if (createTableRequest.stageCreate()) {
                return CatalogHandlers.stageTableCreate(catalog, ns, createTableRequest);
              } else {
                return CatalogHandlers.createTable(catalog, ns, createTableRequest);
              }
            },
            catalogOperationResult -> {
              log.info(
                  "CREATE TABLE RESPONSE, excluding creds, {}",
                  catalogOperationResult.getResponse());
              return includeCreds(
                  platformInstance,
                  xIcebergAccessDelegation,
                  catalogOperationResult.getResponse(),
                  PoliciesConfig.DATA_READ_WRITE_PRIVILEGE,
                  catalogOperationResult.getStorageProviderCredentials());
            });

    return createTableResponse;
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
      Map<String, String> creds = credentialProvider.get(cacheKey, storageProviderCredentials);
      log.info(
          "STS creds {} for primary table location {}",
          creds,
          loadTableResponse.tableMetadata().location());

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
        "GET TABLE REQUEST {} {}.{} ; access-delegation: {}",
        platformInstance,
        namespace,
        table,
        xIcebergAccessDelegation);

    LoadTableResponse getTableResponse =
        catalogOperation(
            platformInstance,
            request,
            operationContext ->
                authorize(
                    operationContext,
                    platformInstance,
                    tableIdFromString(namespace, table),
                    DataOperation.READ_ONLY,
                    true),
            catalog -> CatalogHandlers.loadTable(catalog, tableIdFromString(namespace, table)),
            catalogOperationResult -> {
              log.info(
                  "GET TABLE RESPONSE, excluding creds, {}", catalogOperationResult.getResponse());
              PoliciesConfig.Privilege privilege = catalogOperationResult.getPrivilege();
              if (privilege == PoliciesConfig.DATA_MANAGE_TABLES_PRIVILEGE) {
                privilege = PoliciesConfig.DATA_READ_WRITE_PRIVILEGE;
              } else if (privilege == PoliciesConfig.DATA_MANAGE_VIEWS_PRIVILEGE) {
                privilege = PoliciesConfig.DATA_READ_ONLY_PRIVILEGE;
              }
              return includeCreds(
                  platformInstance,
                  xIcebergAccessDelegation,
                  catalogOperationResult.getResponse(),
                  privilege,
                  catalogOperationResult.getStorageProviderCredentials());
            });

    return getTableResponse;
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

    log.info("UPDATE TABLE REQUEST {}.{}, body {} ", namespace, table, updateTableRequest);

    LoadTableResponse updateTableResponse =
        catalogOperation(
            platformInstance,
            request,
            operationContext ->
                authorize(
                    operationContext,
                    platformInstance,
                    tableIdFromString(namespace, table),
                    DataOperation.READ_WRITE,
                    false),
            catalog ->
                CatalogHandlers.updateTable(
                    catalog, tableIdFromString(namespace, table), updateTableRequest),
            null);

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

    log.info("DROP TABLE REQUEST ns {} table {}", namespace, table);

    catalogOperation(
        platformInstance,
        request,
        operationContext ->
            authorize(operationContext, platformInstance, DataOperation.MANAGE_TABLES, false),
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
        },
        null);
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
    log.info("REGISTER TABLE REQUEST {}", registerTableRequest);

    LoadTableResponse registerTableResponse =
        catalogOperation(
            platformInstance,
            request,
            operationContext ->
                authorize(operationContext, platformInstance, DataOperation.MANAGE_TABLES, false),
            catalog -> {
              // ensure namespace exists
              Namespace ns = namespaceFromString(namespace);
              catalog.loadNamespaceMetadata(ns);
              return CatalogHandlers.registerTable(catalog, ns, registerTableRequest);
            },
            null);

    log.info("REGISTER TABLE RESPONSE {}", registerTableResponse);
    return registerTableResponse;
  }
}
