import React, { Suspense, useState } from 'react';
import { Route, Switch } from 'react-router-dom';

import { DomainsContext, UpdatedDomain } from '@app/domainV2/DomainsContext';
import ManageDomainsSidebar from '@app/domainV2/nestedDomains/ManageDomainsSidebar';
import { GenericEntityProperties } from '@app/entity/shared/types';
import { SuspenseBlock } from '@app/shared/SuspenseBlock';
import { lazyDefault, lazyNamed } from '@app/shared/lazyPage';
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

const EntityPage = lazyNamed(() => import('@app/entityV2/EntityPage'), 'EntityPage');
const ManageDomainsPageV2 = lazyDefault(() => import('@app/domainV2/nestedDomains/ManageDomainsPageV2'));

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
                    <Suspense fallback={<SuspenseBlock />}>
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
                    </Suspense>
                </HierarchicalBrowseMainContent>
            </HierarchicalBrowseContentWrapper>
        </DomainsContext.Provider>
    );
}
