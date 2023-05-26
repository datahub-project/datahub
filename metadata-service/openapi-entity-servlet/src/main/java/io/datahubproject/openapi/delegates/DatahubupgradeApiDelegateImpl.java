package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DatahubupgradeApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatahubupgradeApiDelegateImpl extends AbstractEntityApiImpl<DataHubUpgradeEntityRequestV2,
        DataHubUpgradeEntityResponseV2> implements DatahubupgradeApiDelegate {

    @Override
    String getEntityType() {
        return "dataHubUpgrade";
    }

    @Override
    public ResponseEntity<List<DataHubUpgradeEntityResponseV2>> create(List<DataHubUpgradeEntityRequestV2> body) {
        return createEntity(body, DataHubUpgradeEntityRequestV2.class, DataHubUpgradeEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataHubUpgradeEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataHubUpgradeEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
