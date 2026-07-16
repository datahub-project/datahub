import React, { useCallback, useState } from 'react';
import { Route, Switch, matchPath, useLocation } from 'react-router-dom';
import styled from 'styled-components/macro';

import { EntityPage as EntityPageV2 } from '@app/entityV2/EntityPage';
import { MetricsEntityContextProvider } from '@app/metrics/context/MetricsEntityContext';
import MetricsPage from '@app/metrics/MetricsPage';
import MetricsSidebar, { SIDEBAR_COLLAPSED_WIDTH } from '@app/metrics/MetricsSidebar';
import useSidebarWidth from '@app/sharedV2/sidebar/useSidebarWidth';
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

export default function MetricsRoutes() {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const location = useLocation();
    const [isCollapsed, setIsCollapsed] = useState(false);
    const expandedSidebarWidth = useSidebarWidth(0.2);

    const isEntityProfile =
        matchPath(location.pathname, {
            path: [`${PageRoutes.METRIC_ENTITY}/:urn`, `${PageRoutes.SEMANTIC_MODEL_ENTITY}/:urn`],
        }) !== null;

    const toggleCollapsed = useCallback(() => {
        setIsCollapsed((prev) => !prev);
    }, []);

    const expandSidebar = useCallback(() => {
        setIsCollapsed(false);
    }, []);

    const sidebarWidth = isCollapsed ? SIDEBAR_COLLAPSED_WIDTH : expandedSidebarWidth;

    return (
        <MetricsEntityContextProvider>
            <ContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign} $isEntityProfile={isEntityProfile}>
                <MetricsSidebar
                    width={sidebarWidth}
                    isCollapsed={isCollapsed}
                    isEntityProfile={isEntityProfile}
                    onToggleCollapsed={toggleCollapsed}
                    onExpandSidebar={expandSidebar}
                />
                <MainContent>
                    <Switch>
                        <Route
                            path={`${PageRoutes.METRIC_ENTITY}/:urn`}
                            render={() => <EntityPageV2 entityType={EntityType.Metric} />}
                        />
                        <Route
                            path={`${PageRoutes.SEMANTIC_MODEL_ENTITY}/:urn`}
                            render={() => <EntityPageV2 entityType={EntityType.SemanticModel} />}
                        />
                        <Route path={PageRoutes.METRICS} render={() => <MetricsPage />} />
                    </Switch>
                </MainContent>
            </ContentWrapper>
        </MetricsEntityContextProvider>
    );
}
