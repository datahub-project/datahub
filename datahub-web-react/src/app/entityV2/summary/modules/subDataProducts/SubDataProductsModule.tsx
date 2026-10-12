import { InfiniteScrollList } from '@components';
import { Storefront } from '@phosphor-icons/react/dist/csr/Storefront';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { useGetChildDataProductsOfDataProduct } from '@app/entityV2/summary/modules/subDataProducts/useGetChildDataProductsOfDataProduct';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { DataHubPageModuleType, Entity } from '@types';

const DEFAULT_PAGE_SIZE = 10;

export default function SubDataProductsModule(props: ModuleProps) {
    const { t } = useTranslation('modules');
    const { loading, fetchEntities, total } = useGetChildDataProductsOfDataProduct(DEFAULT_PAGE_SIZE);

    return (
        <LargeModule {...props} loading={loading} dataTestId="sub-data-products-module">
            <InfiniteScrollList<Entity>
                fetchData={fetchEntities}
                renderItem={(entity) => (
                    <EntityItem entity={entity} key={entity.urn} moduleType={DataHubPageModuleType.SubDataProducts} />
                )}
                pageSize={DEFAULT_PAGE_SIZE}
                emptyState={
                    <EmptyContent
                        icon={Storefront}
                        title={t('subDataProducts.emptyTitle')}
                        description={t('subDataProducts.emptyDescription')}
                    />
                }
                totalItemCount={total}
            />
        </LargeModule>
    );
}
