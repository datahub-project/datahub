import React, { Suspense, useCallback, useState } from 'react';
import { Route, Switch } from 'react-router-dom';

import MetricsSidebar from '@app/metrics/MetricsSidebar';
import { MetricsEntityContextProvider } from '@app/metrics/context/MetricsEntityContext';
import { SuspenseBlock } from '@app/sharedV2/SuspenseBlock';
import { lazyDefault, lazyNamed } from '@app/sharedV2/lazyPage';
import {
    HierarchicalBrowseContentWrapper,
    HierarchicalBrowseMainContent,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseLayout.components';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

const EntityPageV2 = lazyNamed(() => import('@app/entityV2/EntityPage'), 'EntityPage');
const MetricsPage = lazyDefault(() => import('@app/metrics/MetricsPage'));

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
                    <Suspense fallback={<SuspenseBlock />}>
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
                    </Suspense>
                </HierarchicalBrowseMainContent>
            </HierarchicalBrowseContentWrapper>
        </MetricsEntityContextProvider>
    );
}
