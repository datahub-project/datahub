package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DatahubroleApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatahubroleApiDelegateImpl extends AbstractEntityApiImpl<DataHubRoleEntityRequestV2,
        DataHubRoleEntityResponseV2> implements DatahubroleApiDelegate {

    @Override
    String getEntityType() {
        return "dataHubRole";
    }

    @Override
    public ResponseEntity<List<DataHubRoleEntityResponseV2>> create(List<DataHubRoleEntityRequestV2> body) {
        return createEntity(body, DataHubRoleEntityRequestV2.class, DataHubRoleEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataHubRoleEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataHubRoleEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
