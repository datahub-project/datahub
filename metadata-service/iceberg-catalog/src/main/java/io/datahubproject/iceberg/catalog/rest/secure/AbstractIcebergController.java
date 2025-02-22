package io.datahubproject.iceberg.catalog.rest.secure;

import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_ENTITY_NAME;
import static io.datahubproject.iceberg.catalog.Utils.*;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.iceberg.catalog.DataHubIcebergWarehouse;
import io.datahubproject.iceberg.catalog.DataHubRestCatalog;
import io.datahubproject.iceberg.catalog.DataOperation;
import io.datahubproject.iceberg.catalog.credentials.CredentialProvider;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.services.SecretService;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
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
  @Autowired protected EntityService entityService;
  @Autowired private EntitySearchService searchService;
  @Autowired private SecretService secretService;

  @Inject
  @Named("cachingCredentialProvider")
  private CredentialProvider cachingCredentialProvider;

  @Inject
  @Named("authorizerChain")
  private Authorizer authorizer;

  @Inject
  @Named("systemOperationContext")
  protected OperationContext systemOperationContext;

  protected PoliciesConfig.Privilege authorize(
      OperationContext operationContext,
      DataHubIcebergWarehouse warehouse,
      TableIdentifier tableIdentifier,
      DataOperation operation,
      boolean returnHighestPrivilege) {
    Optional<DatasetUrn> urn = warehouse.getDatasetUrn(tableIdentifier);
    if (urn.isEmpty()) {
      throw noSuchEntityException(warehouse.getPlatformInstance(), tableIdentifier);
    }

    EntitySpec entitySpec = new EntitySpec(DATASET_ENTITY_NAME, urn.get().toString());
    try {
      return authorize(
          operationContext,
          entitySpec,
          platformInstanceEntitySpec(warehouse.getPlatformInstance()),
          operation,
          returnHighestPrivilege);
    } catch (ForbiddenException e) {
      // specify table id in error message instead of dataset-urn
      throw new ForbiddenException(
          "Data operation %s not authorized on %s",
          operation, fullTableName(warehouse.getPlatformInstance(), tableIdentifier));
    }
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
      String platformInstance, Function<DataHubRestCatalog, R> function) {
    DataHubIcebergWarehouse warehouse =
        DataHubIcebergWarehouse.of(
            platformInstance, entityService, secretService, systemOperationContext);
    return catalogOperation(warehouse, systemOperationContext, function);
  }

  protected <R> R catalogOperation(
      DataHubIcebergWarehouse warehouse,
      OperationContext operationContext,
      Function<DataHubRestCatalog, R> function) {
    DataHubRestCatalog catalog = catalog(operationContext, warehouse);
    try {
      return function.apply(catalog);
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
      OperationContext operationContext, DataHubIcebergWarehouse warehouse) {
    DataHubRestCatalog catalog =
        new DataHubRestCatalog(
            entityService, searchService, operationContext, warehouse, cachingCredentialProvider);
    return catalog;
  }

  protected DataHubIcebergWarehouse warehouse(
      String platformInstance, OperationContext operationContext) {
    return DataHubIcebergWarehouse.of(
        platformInstance, entityService, secretService, operationContext);
  }

  protected RuntimeException noSuchEntityException(
      String platformInstance, TableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException();
  }
}
