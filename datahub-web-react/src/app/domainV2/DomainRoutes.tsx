import React, { useState } from 'react';
import { Route, Switch } from 'react-router-dom';
import styled from 'styled-components/macro';
import { PageRoutes } from '../../conf/Global';
import { EntityType } from '../../types.generated';
import { EntityPage } from '../entity/EntityPage';
import { GenericEntityProperties } from '../entity/shared/types';
import EntitySidebarContext from '../shared/EntitySidebarContext';
import { useEntityRegistry } from '../useEntityRegistry';
import { DomainsContext } from './DomainsContext';
import ManageDomainsPageV2 from './nestedDomains/ManageDomainsPageV2';
import ManageDomainsSidebar from './nestedDomains/ManageDomainsSidebar';

const ContentWrapper = styled.div`
    display: flex;
    flex: 1;
    overflow: hidden;
    border-radius: 8px;
`;

export default function DomainRoutes() {
    const entityRegistry = useEntityRegistry();
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [parentDomainsToUpdate, setParentDomainsToUpdate] = useState<string[]>([]);
    const [isSidebarClosed, setIsSidebarClosed] = useState(false);

    return (
        <DomainsContext.Provider value={{ entityData, setEntityData, parentDomainsToUpdate, setParentDomainsToUpdate }}>
            <ContentWrapper>
                <ManageDomainsSidebar />
                <Switch>
                    <EntitySidebarContext.Provider
                        value={{
                            isClosed: isSidebarClosed,
                            setSidebarClosed: setIsSidebarClosed,
                        }}
                    >
                        <Route
                            key={entityRegistry.getPathName(EntityType.Domain)}
                            path={`/${entityRegistry.getPathName(EntityType.Domain)}/:urn`}
                            render={() => <EntityPage entityType={EntityType.Domain} />}
                        />
                        <Route path={PageRoutes.DOMAINS} render={() => <ManageDomainsPageV2 />} />
                    </EntitySidebarContext.Provider>
                </Switch>
            </ContentWrapper>
        </DomainsContext.Provider>
    );
}
