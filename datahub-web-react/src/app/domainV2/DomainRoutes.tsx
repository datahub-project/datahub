import React, { useState } from 'react';
import { matchPath, Route, Switch, useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';
import { PageRoutes } from '../../conf/Global';
import { EntityType } from '../../types.generated';
import { EntityPage } from '../entity/EntityPage';
import { GenericEntityProperties } from '../entity/shared/types';
import EntitySidebarContext from '../sharedV2/EntitySidebarContext';
import useSidebarWidth from '../sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '../useEntityRegistry';
import { DomainsContext } from './DomainsContext';
import ManageDomainsPageV2 from './nestedDomains/ManageDomainsPageV2';
import ManageDomainsSidebar from './nestedDomains/ManageDomainsSidebar';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

const ContentWrapper = styled.div<{ $isShowNavBarRedesign?: boolean; $isEntityProfile?: boolean }>`
    display: flex;
    overflow: hidden;
    border-radius: 8px;
    flex: 1;
    ${(props) => !props.$isEntityProfile && props.$isShowNavBarRedesign && 'padding: 5px;'}
`;

export default function DomainRoutes() {
    const entityRegistry = useEntityRegistry();
    const [entityData, setEntityData] = useState<GenericEntityProperties | null>(null);
    const [isSidebarClosed, setIsSidebarClosed] = useState(true);
    const entitySidebarWidth = useSidebarWidth();
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const location = useLocation();
    const isEntityProfile =
        matchPath(location.pathname, `/${entityRegistry.getPathName(EntityType.Domain)}/:urn`) !== null;

    return (
        <DomainsContext.Provider value={{ entityData, setEntityData }}>
            <ContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign} $isEntityProfile={isEntityProfile}>
                <ManageDomainsSidebar isEntityProfile={isEntityProfile} />
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
            </ContentWrapper>
        </DomainsContext.Provider>
    );
}
