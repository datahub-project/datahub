import { InfiniteScrollList } from '@components';
import React from 'react';
import { useHistory } from 'react-router';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useGetChildDataProducts } from '@app/entityV2/summary/modules/dataProducts/useGetChildDataProducts';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { DataHubPageModuleType, Entity } from '@types';

const DEFAULT_PAGE_SIZE = 10;

export default function DataProductsModule(props: ModuleProps) {
    const entityRegistry = useEntityRegistryV2();
    const { urn, entityType } = useEntityData();
    const { loading, fetchEntities, total } = useGetChildDataProducts(DEFAULT_PAGE_SIZE);

    const history = useHistory();

    const navigateToDataProductsTab = () => {
        history.push(`${entityRegistry.getEntityUrl(entityType, urn)}/Data Products`);
    };

    return (
        <LargeModule
            {...props}
            loading={loading}
            onClickViewAll={navigateToDataProductsTab}
            dataTestId="data-products-module"
        >
            <InfiniteScrollList<Entity>
                fetchData={fetchEntities}
                renderItem={(entity) => (
                    <EntityItem entity={entity} key={entity.urn} moduleType={DataHubPageModuleType.DataProducts} />
                )}
                pageSize={DEFAULT_PAGE_SIZE}
                emptyState={
                    <EmptyContent
                        icon="FileText"
                        title="No Data Products"
                        description="Create a data product underneath this domain to see it in this list"
                        linkText="Create some data products for this domain"
                        onLinkClick={navigateToDataProductsTab}
                    />
                }
                totalItemCount={total}
            />
        </LargeModule>
    );
}
