package io.datahubproject.openapi.delegates;

import io.datahubproject.openapi.generated.*;
import io.datahubproject.openapi.generated.controller.DataflowApiDelegate;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DataflowApiDelegateImpl extends AbstractEntityApiImpl<DataFlowEntityRequestV2,
        DataFlowEntityResponseV2> implements DataflowApiDelegate {

    @Override
    String getEntityType() {
        return "dataFlow";
    }

    @Override
    public ResponseEntity<List<DataFlowEntityResponseV2>> create(List<DataFlowEntityRequestV2> body) {
        return createEntity(body, DataFlowEntityRequestV2.class, DataFlowEntityResponseV2.class);
    }

    @Override
    public ResponseEntity<Void> delete(String urn) {
        return deleteEntity(urn);
    }

    @Override
    public ResponseEntity<DataFlowEntityResponseV2> get(String urn, Boolean systemMetadata, List<String> aspects) {
        return getEntity(urn, systemMetadata, aspects, DataFlowEntityResponseV2.class);
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
        return getAspect(urn, systemMetadata, methodNameToAspectName(methodName), DataFlowEntityResponseV2.class,
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
