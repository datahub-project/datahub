import GenericEntityPage, { GENERIC_ENTITY_PAGE_PATH } from '@app/GenericEntityPage';
import TypedEntityPage from '@app/TypedEntityPage';
import React, { useState } from 'react';
import { matchPath, Route, Switch, useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';
import { PageRoutes } from '../../conf/Global';
import { EntityType } from '../../types.generated';
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
        location.pathname.startsWith('/urn') ||
        matchPath(location.pathname, `/${entityRegistry.getPathName(EntityType.Domain)}/:urn`) !== null;

    return (
        <DomainsContext.Provider value={{ entityData, setEntityData }}>
            <ContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign} $isEntityProfile={isEntityProfile}>
                <ManageDomainsSidebar isEntityProfile={isEntityProfile} />
                <EntitySidebarContext.Provider
                    value={{
                        width: entitySidebarWidth,
                        isClosed: isSidebarClosed,
                        setSidebarClosed: setIsSidebarClosed,
                    }}
                >
                    <Switch>
                        <Route
                            path={`${PageRoutes.DOMAIN}${GENERIC_ENTITY_PAGE_PATH}`}
                            render={() => <GenericEntityPage />}
                        />
                        <Route
                            path={`/${entityRegistry.getPathName(EntityType.Domain)}/:urn/:tab?`}
                            render={() => <TypedEntityPage entityType={EntityType.Domain} />}
                        />
                        <Route path={PageRoutes.DOMAINS} render={() => <ManageDomainsPageV2 />} />
                    </Switch>
                </EntitySidebarContext.Provider>
            </ContentWrapper>
        </DomainsContext.Provider>
    );
}
