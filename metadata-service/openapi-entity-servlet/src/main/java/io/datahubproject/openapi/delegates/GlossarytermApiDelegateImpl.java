package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.GlossarytermApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class GlossarytermApiDelegateImpl extends AbstractEntityApiImpl<GlossaryTermEntityRequestV2,
        GlossaryTermEntityResponseV2> implements GlossarytermApiDelegate {

    @Override
    String getEntityType() {
        return "glossaryTerm";
    }

    @Override
    public ResponseEntity<List<GlossaryTermEntityResponseV2>> create(List<GlossaryTermEntityRequestV2> body) {
        return createEntity(body, GlossaryTermEntityRequestV2.class, GlossaryTermEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<GlossaryTermEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, GlossaryTermEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> head(String urn) {
        return headEntity(urn);
    }

    @Override
    public ResponseEntity<DomainsAspectResponseV2> createDomains(String urn, DomainsAspectRequestV2 body) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, DomainsAspectRequestV2.class, DomainsAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<OwnershipAspectResponseV2> createOwnership(String urn, OwnershipAspectRequestV2 body) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return createAspect(urn, methodNameToAspectName(methodName), body, OwnershipAspectRequestV2.class, OwnershipAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> deleteDomains(String urn) {
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
    public ResponseEntity<DomainsAspectResponseV2> getDomains(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), GlossaryTermEntityResponseV2.class,
                DomainsAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<OwnershipAspectResponseV2> getOwnership(String urn, Boolean systemMetadata) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), GlossaryTermEntityResponseV2.class,
                OwnershipAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> headDomains(String urn) {
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
}
