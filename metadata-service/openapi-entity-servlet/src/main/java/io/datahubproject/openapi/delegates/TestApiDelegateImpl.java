package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.TestApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TestApiDelegateImpl extends AbstractEntityApiImpl<TestEntityRequestV2,
        TestEntityResponseV2> implements TestApiDelegate {

    @Override
    String getEntityType() {
        return "test";
    }

    @Override
    public ResponseEntity<List<TestEntityResponseV2>> create(List<TestEntityRequestV2> body) {
        return createEntity(body, TestEntityRequestV2.class, TestEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<TestEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, TestEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
