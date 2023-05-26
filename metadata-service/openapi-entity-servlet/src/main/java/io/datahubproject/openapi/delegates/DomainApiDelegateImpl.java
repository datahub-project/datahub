package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DomainApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DomainApiDelegateImpl extends AbstractEntityApiImpl<DomainEntityRequestV2,
        DomainEntityResponseV2> implements DomainApiDelegate {

    @Override
    String getEntityType() {
        return "domain";
    }

    @Override
    public ResponseEntity<List<DomainEntityResponseV2>> create(List<DomainEntityRequestV2> body) {
        return createEntity(body, DomainEntityRequestV2.class, DomainEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DomainEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DomainEntityResponseV2.class);
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
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), DomainEntityResponseV2.class,
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
