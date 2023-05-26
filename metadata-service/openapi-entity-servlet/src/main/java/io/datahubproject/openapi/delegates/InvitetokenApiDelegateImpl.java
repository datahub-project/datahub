package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.InvitetokenApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class InvitetokenApiDelegateImpl extends AbstractEntityApiImpl<InviteTokenEntityRequestV2,
        InviteTokenEntityResponseV2> implements InvitetokenApiDelegate {

    @Override
    String getEntityType() {
        return "inviteToken";
    }

    @Override
    public ResponseEntity<List<InviteTokenEntityResponseV2>> create(List<InviteTokenEntityRequestV2> body) {
        return createEntity(body, InviteTokenEntityRequestV2.class, InviteTokenEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<InviteTokenEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, InviteTokenEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }
}
