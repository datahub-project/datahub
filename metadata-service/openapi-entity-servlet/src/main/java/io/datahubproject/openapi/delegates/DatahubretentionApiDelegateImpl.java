package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DatahubretentionApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatahubretentionApiDelegateImpl extends AbstractEntityApiImpl<DataHubRetentionEntityRequestV2,
        DataHubRetentionEntityResponseV2> implements DatahubretentionApiDelegate {

    @Override
    String getEntityType() {
        return "dataHubRetention";
    }

    @Override
    public ResponseEntity<List<DataHubRetentionEntityResponseV2>> create(List<DataHubRetentionEntityRequestV2> body) {
        return createEntity(body, DataHubRetentionEntityRequestV2.class, DataHubRetentionEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataHubRetentionEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataHubRetentionEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
