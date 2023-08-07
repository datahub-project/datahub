package com.linkedin.metadata.test.action.domain;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.service.DomainService;
import com.linkedin.metadata.test.action.ActionParameters;
import com.linkedin.metadata.test.action.api.ValuesAction;
import com.linkedin.metadata.test.exception.InvalidOperandException;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;

import static com.linkedin.metadata.test.action.ActionUtils.getEntityTypeToUrns;

@RequiredArgsConstructor
public abstract class DomainAbstractAction extends ValuesAction {

    protected final DomainService domainService;

    @Override
    public void apply(List<Urn> urns, ActionParameters params) throws InvalidOperandException {
        // For each entity type, group then apply the action.
        final List<String> domainUrnStrs = params.getParams().get(VALUES_PARAM);
        final Map<String, List<Urn>> entityTypesToUrns = getEntityTypeToUrns(urns);
        for (Map.Entry<String, List<Urn>> entityTypeToUrn : entityTypesToUrns.entrySet()) {
            List<Urn> entityUrns = entityTypeToUrn.getValue();
            Urn domainUrn = UrnUtils.getUrn(domainUrnStrs.get(0));
            if (entityUrns.isEmpty()) {
                continue;
            }
            applyInternal(domainUrn, entityUrns);
        }
    }

    abstract void applyInternal(Urn domainUrn, List<Urn> urns);

}
