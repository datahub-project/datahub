import { InfiniteScrollList } from '@components';
import React from 'react';

import { useGetAssets } from '@app/entityV2/summary/modules/assets/useGetAssets';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { DataHubPageModuleType, Entity } from '@types';

const DEFAULT_PAGE_SIZE = 10;

export default function AssetsModule(props: ModuleProps) {
    const { loading, fetchAssets, total, navigateToAssetsTab } = useGetAssets();

    return (
        <LargeModule {...props} loading={loading} onClickViewAll={navigateToAssetsTab} dataTestId="your-assets-module">
            <div data-testid="user-owned-entities">
                <InfiniteScrollList<Entity>
                    fetchData={fetchAssets}
                    renderItem={(entity) => (
                        <EntityItem entity={entity} key={entity.urn} moduleType={DataHubPageModuleType.OwnedAssets} />
                    )}
                    pageSize={DEFAULT_PAGE_SIZE}
                    emptyState={
                        <EmptyContent
                            icon="Database"
                            title="No Assets"
                            description="Add assets to the parent entity to view them"
                            linkText="Add assets"
                            onLinkClick={navigateToAssetsTab}
                        />
                    }
                    totalItemCount={total}
                />
            </div>
        </LargeModule>
    );
}
