package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DatahubstepstateApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatahubstepstateApiDelegateImpl extends AbstractEntityApiImpl<DataHubStepStateEntityRequestV2,
        DataHubStepStateEntityResponseV2> implements DatahubstepstateApiDelegate {

    @Override
    String getEntityType() {
        return "dataHubStepState";
    }

    @Override
    public ResponseEntity<List<DataHubStepStateEntityResponseV2>> create(List<DataHubStepStateEntityRequestV2> body) {
        return createEntity(body, DataHubStepStateEntityRequestV2.class, DataHubStepStateEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataHubStepStateEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataHubStepStateEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
