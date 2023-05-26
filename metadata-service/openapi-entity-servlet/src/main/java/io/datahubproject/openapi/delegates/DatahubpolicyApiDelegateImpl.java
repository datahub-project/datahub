package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DatahubpolicyApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatahubpolicyApiDelegateImpl extends AbstractEntityApiImpl<DataHubPolicyEntityRequestV2,
        DataHubPolicyEntityResponseV2> implements DatahubpolicyApiDelegate {

    @Override
    String getEntityType() {
        return "dataHubPolicy";
    }

    @Override
    public ResponseEntity<List<DataHubPolicyEntityResponseV2>> create(List<DataHubPolicyEntityRequestV2> body) {
        return createEntity(body, DataHubPolicyEntityRequestV2.class, DataHubPolicyEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataHubPolicyEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataHubPolicyEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
