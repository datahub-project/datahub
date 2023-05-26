package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.CorpuserApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CorpuserApiDelegateImpl extends AbstractEntityApiImpl<CorpuserEntityRequestV2,
        CorpuserEntityResponseV2> implements CorpuserApiDelegate {

    @Override
    String getEntityType() {
        return "corpuser";
    }

    @Override
    public ResponseEntity<List<CorpuserEntityResponseV2>> create(List<CorpuserEntityRequestV2> body) {
        return createEntity(body, CorpuserEntityRequestV2.class, CorpuserEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<CorpuserEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, CorpuserEntityResponseV2.class);
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
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), CorpuserEntityResponseV2.class,
                GlobalTagsAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<StatusAspectResponseV2> getStatus(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), CorpuserEntityResponseV2.class,
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
    public ResponseEntity<Void> headStatus(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }
}
