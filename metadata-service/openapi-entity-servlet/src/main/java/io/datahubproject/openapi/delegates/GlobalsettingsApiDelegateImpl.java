package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.GlobalsettingsApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class GlobalsettingsApiDelegateImpl extends AbstractEntityApiImpl<GlobalSettingsEntityRequestV2,
        GlobalSettingsEntityResponseV2> implements GlobalsettingsApiDelegate {

    @Override
    String getEntityType() {
        return "globalSettings";
    }

    @Override
    public ResponseEntity<List<GlobalSettingsEntityResponseV2>> create(List<GlobalSettingsEntityRequestV2> body) {
        return createEntity(body, GlobalSettingsEntityRequestV2.class, GlobalSettingsEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<GlobalSettingsEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, GlobalSettingsEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
