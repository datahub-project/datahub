package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DataplatformApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DataplatformApiDelegateImpl extends AbstractEntityApiImpl<DataPlatformEntityRequestV2,
        DataPlatformEntityResponseV2> implements DataplatformApiDelegate {

    @Override
    String getEntityType() {
        return "dataPlatform";
    }

    @Override
    public ResponseEntity<List<DataPlatformEntityResponseV2>> create(List<DataPlatformEntityRequestV2> body) {
        return createEntity(body, DataPlatformEntityRequestV2.class, DataPlatformEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataPlatformEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataPlatformEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
