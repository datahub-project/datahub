package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DatahubaccesstokenApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DatahubaccesstokenApiDelegateImpl extends AbstractEntityApiImpl<DataHubAccessTokenEntityRequestV2,
        DataHubAccessTokenEntityResponseV2> implements DatahubaccesstokenApiDelegate {

    @Override
    String getEntityType() {
        return "dataHubAccessToken";
    }

    @Override
    public ResponseEntity<List<DataHubAccessTokenEntityResponseV2>> create(List<DataHubAccessTokenEntityRequestV2> body) {
        return createEntity(body, DataHubAccessTokenEntityRequestV2.class, DataHubAccessTokenEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataHubAccessTokenEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataHubAccessTokenEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
