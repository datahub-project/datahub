import React, { Suspense, useCallback, useState } from 'react';
import { Route, Switch } from 'react-router-dom';

import MarketplaceSidebar from '@app/marketplace/MarketplaceSidebar';
import { MarketplaceEntityContextProvider } from '@app/marketplace/context/MarketplaceEntityContext';
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
const MarketplacePage = lazyDefault(() => import('@app/marketplace/MarketplacePage'));

export default function MarketplaceRoutes() {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [isCollapsed, setIsCollapsed] = useState(false);

    const toggleCollapsed = useCallback(() => {
        setIsCollapsed((prev) => !prev);
    }, []);

    const expandSidebar = useCallback(() => {
        setIsCollapsed(false);
    }, []);

    return (
        <MarketplaceEntityContextProvider>
            <HierarchicalBrowseContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
                <MarketplaceSidebar
                    isCollapsed={isCollapsed}
                    onToggleCollapsed={toggleCollapsed}
                    onExpandSidebar={expandSidebar}
                />
                <HierarchicalBrowseMainContent>
                    <Suspense fallback={<SuspenseBlock />}>
                        <Switch>
                            <Route
                                path={`${PageRoutes.DATA_PRODUCT_ENTITY}/:urn`}
                                render={() => <EntityPageV2 entityType={EntityType.DataProduct} />}
                            />
                            <Route path={PageRoutes.MARKETPLACE} render={() => <MarketplacePage />} />
                        </Switch>
                    </Suspense>
                </HierarchicalBrowseMainContent>
            </HierarchicalBrowseContentWrapper>
        </MarketplaceEntityContextProvider>
    );
}
