import React, { useCallback, useState } from 'react';
import { Route, Switch, matchPath, useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';

import ContextDocumentsPage from '@app/context/ContextDocumentsPage';
import { ContextLayoutProvider } from '@app/context/ContextLayoutContext';
import ContextSidebar, { SIDEBAR_COLLAPSED_WIDTH } from '@app/context/ContextSidebar';
import { EntityPage as EntityPageV2 } from '@app/entityV2/EntityPage';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

const ContentWrapper = styled.div<{ $isShowNavBarRedesign: boolean; $isEntityProfile: boolean }>`
    display: flex;
    flex: 1;
    overflow: hidden;
    gap: ${(props) => (props.$isShowNavBarRedesign ? '12px' : '0')};
    ${(props) => !props.$isEntityProfile && props.$isShowNavBarRedesign && 'padding: 5px;'}
`;

const MainContent = styled.div`
    flex: 1;
    overflow: hidden;
    display: flex;
    flex-direction: column;
`;

/**
 * ContextRoutes - Routes for the Context Documents section
 *
 * Layout: Sidebar on left (document tree) + Content on right (document profile or landing page)
 *
 * Routes:
 * - /context/documents -> ContextDocumentsPage (redirects to first doc or creates one)
 * - /document/:urn -> Document profile
 */
export default function ContextRoutes() {
    const entityRegistry = useEntityRegistry();
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const location = useLocation();
    const [isCollapsed, setIsCollapsed] = useState(false);
    const expandedSidebarWidth = useSidebarWidth(0.2);

    // Check if we're on an entity profile page (document/:urn)
    const documentPath = `/${entityRegistry.getPathName(EntityType.Document)}/:urn`;
    const isEntityProfile = matchPath(location.pathname, { path: documentPath }) !== null;

    const toggleCollapsed = useCallback(() => {
        setIsCollapsed((prev) => !prev);
    }, []);

    const expandSidebar = useCallback(() => {
        setIsCollapsed(false);
    }, []);

    // Calculate the sidebar width for the layout context
    const sidebarWidth = isCollapsed ? SIDEBAR_COLLAPSED_WIDTH : expandedSidebarWidth;

    return (
        <ContextLayoutProvider sidebarWidth={sidebarWidth}>
            <ContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign} $isEntityProfile={isEntityProfile}>
                <ContextSidebar
                    isEntityProfile={isEntityProfile}
                    isCollapsed={isCollapsed}
                    onToggleCollapsed={toggleCollapsed}
                    onExpandSidebar={expandSidebar}
                />
                <MainContent>
                    <Switch>
                        <Route path={documentPath} render={() => <EntityPageV2 entityType={EntityType.Document} />} />
                        <Route path={PageRoutes.CONTEXT_DOCUMENTS} render={() => <ContextDocumentsPage />} />
                    </Switch>
                </MainContent>
            </ContentWrapper>
        </ContextLayoutProvider>
    );
}
