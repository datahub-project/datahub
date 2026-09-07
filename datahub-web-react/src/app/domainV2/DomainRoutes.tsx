import React, { useState } from 'react';
import { Route, Switch } from 'react-router-dom';

import { DomainsContext, UpdatedDomain } from '@app/domainV2/DomainsContext';
import ManageDomainsPageV2 from '@app/domainV2/nestedDomains/ManageDomainsPageV2';
import ManageDomainsSidebar from '@app/domainV2/nestedDomains/ManageDomainsSidebar';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { EntityPage } from '@app/entityV2/EntityPage';
import EntitySidebarContext from '@app/sharedV2/EntitySidebarContext';
import {
    HierarchicalBrowseContentWrapper,
    HierarchicalBrowseMainContent,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseLayout.components';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

export default function DomainRoutes() {
    const entityRegistry = useEntityRegistry();
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [newDomain, setNewDomain] = useState<UpdatedDomain | null>(null);
    const [deletedDomain, setDeletedDomain] = useState<UpdatedDomain | null>(null);
    const [updatedDomain, setUpdatedDomain] = useState<UpdatedDomain | null>(null);
    const [isSidebarClosed, setIsSidebarClosed] = useState(true);
    const entitySidebarWidth = useSidebarWidth();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    return (
        <DomainsContext.Provider
            value={{
                entityData,
                setEntityData,
                newDomain,
                setNewDomain,
                deletedDomain,
                setDeletedDomain,
                updatedDomain,
                setUpdatedDomain,
            }}
        >
            <HierarchicalBrowseContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
                <ManageDomainsSidebar />
                <HierarchicalBrowseMainContent>
                    <Switch>
                        <EntitySidebarContext.Provider
                            value={{
                                width: entitySidebarWidth,
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
                </HierarchicalBrowseMainContent>
            </HierarchicalBrowseContentWrapper>
        </DomainsContext.Provider>
    );
}
