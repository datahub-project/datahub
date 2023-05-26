package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.AssertionApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AssertionApiDelegateImpl extends AbstractEntityApiImpl<AssertionEntityRequestV2,
        AssertionEntityResponseV2> implements AssertionApiDelegate {

    @Override
    String getEntityType() {
        return "assertion";
    }

    @Override
    public ResponseEntity<List<AssertionEntityResponseV2>> create(List<AssertionEntityRequestV2> body) {
        return createEntity(body, AssertionEntityRequestV2.class, AssertionEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<AssertionEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, AssertionEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }

    @Override
    public ResponseEntity<StatusAspectResponseV2> createStatus(String urn, StatusAspectRequestV2 body) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, StatusAspectRequestV2.class, StatusAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> deleteStatus(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return deleteAspect(urn, methodNameToAspectName(methodName));
    }

    @Override
    public ResponseEntity<StatusAspectResponseV2> getStatus(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), AssertionEntityResponseV2.class,
                StatusAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> headStatus(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }
}
