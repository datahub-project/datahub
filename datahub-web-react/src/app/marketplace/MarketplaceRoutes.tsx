import React, { useCallback, useState } from 'react';
import { Route, Switch } from 'react-router-dom';

import { EntityPage as EntityPageV2 } from '@app/entityV2/EntityPage';
import MarketplacePage from '@app/marketplace/MarketplacePage';
import MarketplaceSidebar from '@app/marketplace/MarketplaceSidebar';
import { MarketplaceEntityContextProvider } from '@app/marketplace/context/MarketplaceEntityContext';
import {
    HierarchicalBrowseContentWrapper,
    HierarchicalBrowseMainContent,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseLayout.components';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

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
                    <Switch>
                        <Route
                            path={`${PageRoutes.DATA_PRODUCT_ENTITY}/:urn`}
                            render={() => <EntityPageV2 entityType={EntityType.DataProduct} />}
                        />
                        <Route path={PageRoutes.MARKETPLACE} render={() => <MarketplacePage />} />
                    </Switch>
                </HierarchicalBrowseMainContent>
            </HierarchicalBrowseContentWrapper>
        </MarketplaceEntityContextProvider>
    );
}
