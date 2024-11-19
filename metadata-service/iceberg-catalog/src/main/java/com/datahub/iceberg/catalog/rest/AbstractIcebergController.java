package com.datahub.iceberg.catalog.rest;

import static com.datahub.iceberg.catalog.Utils.*;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.EntitySpec;
import com.datahub.iceberg.catalog.CredentialProvider;
import com.datahub.iceberg.catalog.DataHubRestCatalog;
import com.datahub.iceberg.catalog.DataOperation;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class AbstractIcebergController {
  @Autowired private EntityService entityService;
  @Autowired protected CredentialProvider credentialProvider;

  @Inject
  @Named("authorizerChain")
  private Authorizer authorizer;

  @Inject
  @Named("systemOperationContext")
  private OperationContext systemOperationContext;

  protected PoliciesConfig.Privilege authorize(
      OperationContext operationContext,
      String platformInstance,
      TableIdentifier tableIdentifier,
      DataOperation operation,
      boolean returnHighestPrivilege) {
    DatasetUrn urn = datasetUrn(platformInstance, tableIdentifier);
    EntitySpec entitySpec = new EntitySpec(DATASET_ENTITY_NAME, urn.toString());
    return authorize(
        operationContext,
        entitySpec,
        platformInstanceEntitySpec(platformInstance),
        operation,
        returnHighestPrivilege);
  }

  protected PoliciesConfig.Privilege authorize(
      OperationContext operationContext,
      String platformInstance,
      DataOperation operation,
      boolean returnHighestPrivilege) {
    EntitySpec entitySpec = platformInstanceEntitySpec(platformInstance);
    return authorize(operationContext, entitySpec, entitySpec, operation, returnHighestPrivilege);
  }

  private EntitySpec platformInstanceEntitySpec(String platformInstance) {
    Urn urn = platformInstanceUrn(platformInstance);
    return new EntitySpec(DATA_PLATFORM_INSTANCE_ENTITY_NAME, urn.toString());
  }

  private PoliciesConfig.Privilege authorize(
      OperationContext operationContext,
      EntitySpec entitySpec,
      EntitySpec platformInstanceEntitySpec,
      DataOperation operation,
      boolean returnHighestPrivilege) {
    List<PoliciesConfig.Privilege> privileges =
        returnHighestPrivilege ? operation.descendingPrivileges : operation.ascendingPrivileges;

    for (PoliciesConfig.Privilege privilege : privileges) {
      if ((entitySpec.getType().equals(DATASET_ENTITY_NAME)
              && PoliciesConfig.DATASET_PRIVILEGES.getPrivileges().contains(privilege)
          || (entitySpec.getType().equals(DATA_PLATFORM_INSTANCE_ENTITY_NAME)
              && PoliciesConfig.PLATFORM_INSTANCE_PRIVILEGES
                  .getPrivileges()
                  .contains(privilege)))) {
        if (AuthUtil.isAuthorized(operationContext, privilege, entitySpec)) {
          return privilege;
        }
      } else if (entitySpec.getType().equals(DATASET_ENTITY_NAME)
          && PoliciesConfig.PLATFORM_INSTANCE_PRIVILEGES.getPrivileges().contains(privilege)) {
        if (AuthUtil.isAuthorized(operationContext, privilege, platformInstanceEntitySpec)) {
          return privilege;
        }
      }
    }

    throw new ForbiddenException("Data operation %s not authorized on %s", operation, entitySpec);
  }

  @Data
  @AllArgsConstructor
  protected static class CatalogOperationResult<R> {
    private R response;
    private PoliciesConfig.Privilege privilege;
    private CredentialProvider.StorageProviderCredentials storageProviderCredentials;
  }

  protected <R> R catalogOperation(
      String platformInstance,
      HttpServletRequest request,
      Function<OperationContext, PoliciesConfig.Privilege> authorizer,
      Function<DataHubRestCatalog, R> function,
      Function<CatalogOperationResult<R>, R> includeCreds) {
    OperationContext operationContext = opContext(request);
    PoliciesConfig.Privilege privilege = authorizer.apply(operationContext);

    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(platformInstance, entityService, operationContext);
    DataHubRestCatalog catalog = catalog(operationContext, warehouse, platformInstance);
    try {
      R response = function.apply(catalog);
      if (includeCreds == null) {
        return response;
      } else {
        CatalogOperationResult<R> operationResult =
            new CatalogOperationResult<>(
                response, privilege, warehouse.getStorageProviderCredentials());
        return includeCreds.apply(operationResult);
      }
    } finally {
      try {
        catalog.close();
      } catch (IOException e) {
        log.error("Error while closing catalog", e);
      }
    }
  }

  protected OperationContext opContext(HttpServletRequest request) {
    Authentication auth = AuthenticationContext.getAuthentication();
    return OperationContext.asSession(
        systemOperationContext,
        RequestContext.builder()
            .buildOpenapi(auth.getActor().toUrnStr(), request, "icebergDataAction", "dataset"),
        authorizer,
        auth,
        true);
  }

  protected DataHubRestCatalog catalog(
      OperationContext operationContext,
      DataHubIcebergWarehouse warehouse,
      String platformInstance) {
    DataHubRestCatalog catalog =
        new DataHubRestCatalog(entityService, operationContext, warehouse, credentialProvider);
    catalog.initialize(platformInstance, Collections.emptyMap());
    return catalog;
  }
}
