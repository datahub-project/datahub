package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.CorpgroupApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CorpgroupApiDelegateImpl extends AbstractEntityApiImpl<CorpGroupEntityRequestV2,
        CorpGroupEntityResponseV2> implements CorpgroupApiDelegate {

    @Override
    String getEntityType() {
        return "corpGroup";
    }

    @Override
    public ResponseEntity<List<CorpGroupEntityResponseV2>> create(List<CorpGroupEntityRequestV2> body) {
        return createEntity(body, CorpGroupEntityRequestV2.class, CorpGroupEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<CorpGroupEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, CorpGroupEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }

    @Override
    public ResponseEntity<GlobalTagsAspectResponseV2> createGlobalTags(String urn, GlobalTagsAspectRequestV2 body) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, GlobalTagsAspectRequestV2.class, GlobalTagsAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<OwnershipAspectResponseV2> createOwnership(String urn, OwnershipAspectRequestV2 body) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, OwnershipAspectRequestV2.class, OwnershipAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<StatusAspectResponseV2> createStatus(String urn, StatusAspectRequestV2 body) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, StatusAspectRequestV2.class, StatusAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> deleteGlobalTags(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    @Override
    public ResponseEntity<Void> deleteOwnership(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    @Override
    public ResponseEntity<Void> deleteStatus(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    @Override
    public ResponseEntity<GlobalTagsAspectResponseV2> getGlobalTags(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), CorpGroupEntityResponseV2.class,
                GlobalTagsAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<OwnershipAspectResponseV2> getOwnership(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), CorpGroupEntityResponseV2.class,
                OwnershipAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<StatusAspectResponseV2> getStatus(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), CorpGroupEntityResponseV2.class,
                StatusAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> headGlobalTags(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }

    @Override
    public ResponseEntity<Void> headOwnership(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }

    @Override
    public ResponseEntity<Void> headStatus(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }
}
