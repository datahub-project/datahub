package com.datahub.iceberg.catalog.rest;

import com.datahub.iceberg.catalog.CredentialProvider;
import com.datahub.iceberg.catalog.Utils;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatforminstance.IcebergWarehouse;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.secret.DataHubSecretValue;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;

public class DataHubIcebergWarehouse {

  private final EntityService entityService;

  private final OperationContext operationContext;

  private final IcebergWarehouse icebergWarehouse;

  @Getter private final String platformInstance;

  private DataHubIcebergWarehouse(
      String platformInstance,
      IcebergWarehouse icebergWarehouse,
      EntityService entityService,
      OperationContext operationContext) {
    this.platformInstance = platformInstance;
    this.icebergWarehouse = icebergWarehouse;
    this.entityService = entityService;
    this.operationContext = operationContext;
  }

  public static DataHubIcebergWarehouse of(
      String platformInstance, EntityService entityService, OperationContext operationContext) {
    Urn platformInstanceUrn = Utils.platformInstanceUrn(platformInstance);
    RecordTemplate warehouseAspect =
        entityService.getLatestAspect(operationContext, platformInstanceUrn, "icebergWarehouse");

    if (warehouseAspect == null) {
      throw new RuntimeException("Unknown warehouse");
    }

    IcebergWarehouse icebergWarehouse = new IcebergWarehouse(warehouseAspect.data());
    return new DataHubIcebergWarehouse(
        platformInstance, icebergWarehouse, entityService, operationContext);
  }

  public CredentialProvider.StorageProviderCredentials getStorageProviderCredentials() {

    Urn clientIdUrn, clientSecretUrn;
    String role, region;

    clientIdUrn = icebergWarehouse.getClientId();
    clientSecretUrn = icebergWarehouse.getClientSecret();
    role = icebergWarehouse.getRole();
    region = icebergWarehouse.getRegion();

    Map<Urn, List<RecordTemplate>> credsMap =
        entityService.getLatestAspects(
            operationContext, Set.of(clientIdUrn, clientSecretUrn), Set.of("dataHubSecretValue"));

    DataHubSecretValue clientIdValue =
        new DataHubSecretValue(credsMap.get(clientIdUrn).get(1).data());
    String clientId = clientIdValue.getValue();

    DataHubSecretValue clientSecretValue =
        new DataHubSecretValue(credsMap.get(clientSecretUrn).get(1).data());
    String clientSecret = clientSecretValue.getValue();

    return new CredentialProvider.StorageProviderCredentials(clientId, clientSecret, role, region);
  }

  public String getDataRoot() {
    return icebergWarehouse.getDataRoot();
  }
}
