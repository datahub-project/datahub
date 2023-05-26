package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.TagApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TagApiDelegateImpl extends AbstractEntityApiImpl<TagEntityRequestV2,
        TagEntityResponseV2> implements TagApiDelegate {

    @Override
    String getEntityType() {
        return "tag";
    }

    @Override
    public ResponseEntity<List<TagEntityResponseV2>> create(List<TagEntityRequestV2> body) {
        return createEntity(body, TagEntityRequestV2.class, TagEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<TagEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, TagEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }


    @Override
    public ResponseEntity<OwnershipAspectResponseV2> createOwnership(String urn, OwnershipAspectRequestV2 body) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, OwnershipAspectRequestV2.class, OwnershipAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> deleteOwnership(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    @Override
    public ResponseEntity<OwnershipAspectResponseV2> getOwnership(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), TagEntityResponseV2.class,
                OwnershipAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> headOwnership(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }
}
