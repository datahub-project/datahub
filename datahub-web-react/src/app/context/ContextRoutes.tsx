import React, { useCallback, useState } from 'react';
import { Route, Switch, matchPath, useLocation } from 'react-router-dom';

import ContextDocumentsPage from '@app/context/ContextDocumentsPage';
import { ContextLayoutProvider } from '@app/context/ContextLayoutContext';
import ContextSidebar from '@app/context/ContextSidebar';
import { DocumentFiltersProvider } from '@app/document/DocumentFiltersContext';
import { EntityPage as EntityPageV2 } from '@app/entityV2/EntityPage';
import {
    HierarchicalBrowseContentWrapper,
    HierarchicalBrowseMainContent,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseLayout.components';
import { SIDEBAR_COLLAPSED_WIDTH } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import useHierarchicalBrowseSidebarWidth from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/useHierarchicalBrowseSidebarWidth';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

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
    // Start hidden - DocumentProfile controls visibility based on document type
    const [isSidebarHidden, setIsSidebarHidden] = useState(true);
    const { width: expandedSidebarWidth, setWidth: setExpandedSidebarWidth } = useHierarchicalBrowseSidebarWidth();

    // Check if we're on an entity profile page (document/:urn)
    const documentPath = `/${entityRegistry.getPathName(EntityType.Document)}/:urn`;
    const isEntityProfile = matchPath(location.pathname, { path: documentPath }) !== null;

    const toggleCollapsed = useCallback(() => {
        setIsCollapsed((prev) => !prev);
    }, []);

    const expandSidebar = useCallback(() => {
        setIsCollapsed(false);
    }, []);

    const setSidebarHidden = useCallback((hidden: boolean) => {
        setIsSidebarHidden(hidden);
    }, []);

    // Calculate the sidebar width for the layout context
    const getEffectiveSidebarWidth = () => {
        if (isSidebarHidden) return 0;
        if (isCollapsed) return SIDEBAR_COLLAPSED_WIDTH;
        return expandedSidebarWidth;
    };
    const sidebarWidth = getEffectiveSidebarWidth();

    return (
        <ContextLayoutProvider
            sidebarWidth={sidebarWidth}
            isSidebarHidden={isSidebarHidden}
            setSidebarHidden={setSidebarHidden}
        >
            <DocumentFiltersProvider>
                <HierarchicalBrowseContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
                    {!isSidebarHidden && (
                        <ContextSidebar
                            isEntityProfile={isEntityProfile}
                            isCollapsed={isCollapsed}
                            onToggleCollapsed={toggleCollapsed}
                            onExpandSidebar={expandSidebar}
                            onWidthChange={setExpandedSidebarWidth}
                        />
                    )}
                    <HierarchicalBrowseMainContent>
                        <Switch>
                            <Route
                                path={documentPath}
                                render={() => <EntityPageV2 entityType={EntityType.Document} />}
                            />
                            <Route path={PageRoutes.CONTEXT_DOCUMENTS} render={() => <ContextDocumentsPage />} />
                        </Switch>
                    </HierarchicalBrowseMainContent>
                </HierarchicalBrowseContentWrapper>
            </DocumentFiltersProvider>
        </ContextLayoutProvider>
    );
}
