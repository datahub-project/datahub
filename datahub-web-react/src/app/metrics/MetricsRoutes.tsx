import React, { useCallback, useState } from 'react';
import { Route, Switch } from 'react-router-dom';

import { EntityPage as EntityPageV2 } from '@app/entityV2/EntityPage';
import MetricsPage from '@app/metrics/MetricsPage';
import MetricsSidebar from '@app/metrics/MetricsSidebar';
import { MetricsEntityContextProvider } from '@app/metrics/context/MetricsEntityContext';
import {
    HierarchicalBrowseContentWrapper,
    HierarchicalBrowseMainContent,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseLayout.components';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

export default function MetricsRoutes() {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [isCollapsed, setIsCollapsed] = useState(false);

    const toggleCollapsed = useCallback(() => {
        setIsCollapsed((prev) => !prev);
    }, []);

    const expandSidebar = useCallback(() => {
        setIsCollapsed(false);
    }, []);

    return (
        <MetricsEntityContextProvider>
            <HierarchicalBrowseContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
                <MetricsSidebar
                    isCollapsed={isCollapsed}
                    onToggleCollapsed={toggleCollapsed}
                    onExpandSidebar={expandSidebar}
                />
                <HierarchicalBrowseMainContent>
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
                </HierarchicalBrowseMainContent>
            </HierarchicalBrowseContentWrapper>
        </MetricsEntityContextProvider>
    );
}
