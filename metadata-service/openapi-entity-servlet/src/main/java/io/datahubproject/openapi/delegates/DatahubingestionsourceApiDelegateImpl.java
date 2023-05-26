package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DatahubingestionsourceApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatahubingestionsourceApiDelegateImpl extends AbstractEntityApiImpl<DataHubIngestionSourceEntityRequestV2,
        DataHubIngestionSourceEntityResponseV2> implements DatahubingestionsourceApiDelegate {

    @Override
    String getEntityType() {
        return "dataHubIngestionSource";
    }

    @Override
    public ResponseEntity<List<DataHubIngestionSourceEntityResponseV2>> create(List<DataHubIngestionSourceEntityRequestV2> body) {
        return createEntity(body, DataHubIngestionSourceEntityRequestV2.class, DataHubIngestionSourceEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataHubIngestionSourceEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataHubIngestionSourceEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
