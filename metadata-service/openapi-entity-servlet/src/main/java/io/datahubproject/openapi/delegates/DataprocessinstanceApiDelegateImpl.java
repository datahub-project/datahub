package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DataprocessinstanceApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DataprocessinstanceApiDelegateImpl extends AbstractEntityApiImpl<DataProcessInstanceEntityRequestV2,
        DataProcessInstanceEntityResponseV2> implements DataprocessinstanceApiDelegate {

    @Override
    String getEntityType() {
        return "dataProcessInstance";
    }

    @Override
    public ResponseEntity<List<DataProcessInstanceEntityResponseV2>> create(List<DataProcessInstanceEntityRequestV2> body) {
        return createEntity(body, DataProcessInstanceEntityRequestV2.class, DataProcessInstanceEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataProcessInstanceEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataProcessInstanceEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
