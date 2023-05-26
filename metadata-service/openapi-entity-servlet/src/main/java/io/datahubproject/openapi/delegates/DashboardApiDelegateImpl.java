package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DashboardApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DashboardApiDelegateImpl extends AbstractEntityApiImpl<DashboardEntityRequestV2,
        DashboardEntityResponseV2> implements DashboardApiDelegate {

    @Override
    String getEntityType() {
        return "dashboard";
    }

    @Override
    public ResponseEntity<List<DashboardEntityResponseV2>> create(List<DashboardEntityRequestV2> body) {
        return createEntity(body, DashboardEntityRequestV2.class, DashboardEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DashboardEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DashboardEntityResponseV2.class);
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
    public ResponseEntity<Void> deleteDomains(String urn) {
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
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), DashboardEntityResponseV2.class,
                DomainsAspectResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> headDomains(String urn) {
        String methodName = walker.walk(frames -> frames
                .findFirst()
                .map(StackWalker.StackFrame::getMethodName)).get();
        return headAspect(urn, methodNameToAspectName(methodName));
    }
}
