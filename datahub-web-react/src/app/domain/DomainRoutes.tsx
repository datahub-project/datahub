import React, { useState } from 'react';
import { Route, Switch } from 'react-router-dom';
import styled from 'styled-components/macro';

import { DomainsContext } from '@app/domain/DomainsContext';
import ManageDomainsPageV2 from '@app/domain/nestedDomains/ManageDomainsPageV2';
import ManageDomainsSidebar from '@app/domain/nestedDomains/ManageDomainsSidebar';
import { EntityPage } from '@app/entity/EntityPage';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

const ContentWrapper = styled.div`
    display: flex;
    flex: 1;
    overflow: hidden;
`;

export default function DomainRoutes() {
    const entityRegistry = useEntityRegistry();
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [parentDomainsToUpdate, setParentDomainsToUpdate] = useState<string[]>([]);

    return (
        <DomainsContext.Provider value={{ entityData, setEntityData, parentDomainsToUpdate, setParentDomainsToUpdate }}>
            <ContentWrapper>
                <ManageDomainsSidebar />
                <Switch>
                    <Route
                        key={entityRegistry.getPathName(EntityType.Domain)}
                        path={`/${entityRegistry.getPathName(EntityType.Domain)}/:urn`}
                        render={() => <EntityPage entityType={EntityType.Domain} />}
                    />
                    <Route path={PageRoutes.DOMAINS} render={() => <ManageDomainsPageV2 />} />
                </Switch>
            </ContentWrapper>
        </DomainsContext.Provider>
    );
}
