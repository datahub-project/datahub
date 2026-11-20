import { InfiniteScrollList } from '@components';
import React from 'react';

import { useGetChildDataProductsOfDataProduct } from '@app/entityV2/summary/modules/subDataProducts/useGetChildDataProductsOfDataProduct';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { useReportModuleHidden } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps } from '@app/homeV3/module/types';

import { DataHubPageModuleType, Entity } from '@types';

const DEFAULT_PAGE_SIZE = 10;

export default function SubDataProductsModule(props: ModuleProps) {
    const { loading, fetchEntities, total } = useGetChildDataProductsOfDataProduct(DEFAULT_PAGE_SIZE);
    const isHidden = total === 0;
    useReportModuleHidden(isHidden);

    // Child data products are rare, so hide the module entirely rather than showing an empty state
    if (isHidden) {
        return null;
    }

    return (
        <LargeModule {...props} loading={loading} dataTestId="sub-data-products-module">
            <InfiniteScrollList<Entity>
                fetchData={fetchEntities}
                renderItem={(entity) => (
                    <EntityItem entity={entity} key={entity.urn} moduleType={DataHubPageModuleType.SubDataProducts} />
                )}
                pageSize={DEFAULT_PAGE_SIZE}
                totalItemCount={total}
            />
        </LargeModule>
    );
}
