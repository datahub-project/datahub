package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.SchemafieldApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SchemafieldApiDelegateImpl extends AbstractEntityApiImpl<SchemaFieldEntityRequestV2,
        SchemaFieldEntityResponseV2> implements SchemafieldApiDelegate {

    @Override
    String getEntityType() {
        return "schemaField";
    }

    @Override
    public ResponseEntity<List<SchemaFieldEntityResponseV2>> create(List<SchemaFieldEntityRequestV2> body) {
        return createEntity(body, SchemaFieldEntityRequestV2.class, SchemaFieldEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<SchemaFieldEntityResponseV2> get(String urn, Boolean systemMetadata) {
        return getEntity(urn, systemMetadata, List.of(), SchemaFieldEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }

}
