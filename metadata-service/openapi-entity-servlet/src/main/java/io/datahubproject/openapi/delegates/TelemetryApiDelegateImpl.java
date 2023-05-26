package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.TelemetryApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TelemetryApiDelegateImpl extends AbstractEntityApiImpl<TelemetryEntityRequestV2,
        TelemetryEntityResponseV2> implements TelemetryApiDelegate {

    @Override
    String getEntityType() {
        return "telemetry";
    }

    @Override
    public ResponseEntity<List<TelemetryEntityResponseV2>> create(List<TelemetryEntityRequestV2> body) {
        return createEntity(body, TelemetryEntityRequestV2.class, TelemetryEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<TelemetryEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, TelemetryEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
