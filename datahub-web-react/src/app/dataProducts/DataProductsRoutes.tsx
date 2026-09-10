import React, { useCallback, useState } from 'react';
import { Route, Switch } from 'react-router-dom';

import { EntityPage as EntityPageV2 } from '@app/entityV2/EntityPage';
import DataProductsPage from '@app/dataProducts/DataProductsPage';
import DataProductsSidebar from '@app/dataProducts/DataProductsSidebar';
import { DataProductsEntityContextProvider } from '@app/dataProducts/context/DataProductsEntityContext';
import {
    HierarchicalBrowseContentWrapper,
    HierarchicalBrowseMainContent,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseLayout.components';
import { useShowNavBarRedesign } from '@app/useShowNavBarRedesign';
import { PageRoutes } from '@conf/Global';

import { EntityType } from '@types';

export default function DataProductsRoutes() {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const [isCollapsed, setIsCollapsed] = useState(false);

    const toggleCollapsed = useCallback(() => {
        setIsCollapsed((prev) => !prev);
    }, []);

    const expandSidebar = useCallback(() => {
        setIsCollapsed(false);
    }, []);

    return (
        <DataProductsEntityContextProvider>
            <HierarchicalBrowseContentWrapper $isShowNavBarRedesign={isShowNavBarRedesign}>
                <DataProductsSidebar
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
                        <Route path={PageRoutes.DATA_PRODUCTS} render={() => <DataProductsPage />} />
                    </Switch>
                </HierarchicalBrowseMainContent>
            </HierarchicalBrowseContentWrapper>
        </DataProductsEntityContextProvider>
    );
}
