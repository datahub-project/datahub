package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DatahubsecretApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatahubsecretApiDelegateImpl extends AbstractEntityApiImpl<DataHubSecretEntityRequestV2,
        DataHubSecretEntityResponseV2> implements DatahubsecretApiDelegate {

    @Override
    String getEntityType() {
        return "dataHubSecret";
    }

    @Override
    public ResponseEntity<List<DataHubSecretEntityResponseV2>> create(List<DataHubSecretEntityRequestV2> body) {
        return createEntity(body, DataHubSecretEntityRequestV2.class, DataHubSecretEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataHubSecretEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataHubSecretEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
