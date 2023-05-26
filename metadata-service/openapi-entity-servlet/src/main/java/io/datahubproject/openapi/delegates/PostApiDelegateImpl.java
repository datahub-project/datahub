package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.PostApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PostApiDelegateImpl extends AbstractEntityApiImpl<PostEntityRequestV2,
        PostEntityResponseV2> implements PostApiDelegate {

    @Override
    String getEntityType() {
        return "post";
    }

    @Override
    public ResponseEntity<List<PostEntityResponseV2>> create(List<PostEntityRequestV2> body) {
        return createEntity(body, PostEntityRequestV2.class, PostEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<PostEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, PostEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
