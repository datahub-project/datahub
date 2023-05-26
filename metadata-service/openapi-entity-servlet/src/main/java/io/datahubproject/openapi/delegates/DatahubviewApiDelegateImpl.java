package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DatahubviewApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatahubviewApiDelegateImpl extends AbstractEntityApiImpl<DataHubViewEntityRequestV2,
        DataHubViewEntityResponseV2> implements DatahubviewApiDelegate {

    @Override
    String getEntityType() {
        return "dataHubView";
    }

    @Override
    public ResponseEntity<List<DataHubViewEntityResponseV2>> create(List<DataHubViewEntityRequestV2> body) {
        return createEntity(body, DataHubViewEntityRequestV2.class, DataHubViewEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataHubViewEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataHubViewEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
