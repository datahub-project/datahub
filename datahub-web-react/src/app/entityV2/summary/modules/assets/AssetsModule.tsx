/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { InfiniteScrollList } from '@components';
import React, { useState } from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import AddAssetsModal from '@app/entityV2/summary/modules/assets/AddAssetsModal';
import { ENTITIES_TO_ADD_TO_ASSETS } from '@app/entityV2/summary/modules/assets/constants';
import { useGetAssets } from '@app/entityV2/summary/modules/assets/useGetAssets';
import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { DataHubPageModuleType, Entity } from '@types';

const DEFAULT_PAGE_SIZE = 10;

export default function AssetsModule(props: ModuleProps) {
    const { loading, fetchAssets, total, navigateToAssetsTab } = useGetAssets();
    const { entityType } = useEntityData();

    const canAddToAssets = ENTITIES_TO_ADD_TO_ASSETS.includes(entityType);
    const [showAddAssetsModal, setShowAddAssetsModal] = useState(false);

    return (
        <LargeModule {...props} loading={loading} onClickViewAll={navigateToAssetsTab} dataTestId="assets-module">
            <InfiniteScrollList<Entity>
                fetchData={fetchAssets}
                renderItem={(entity) => (
                    <EntityItem entity={entity} key={entity.urn} moduleType={DataHubPageModuleType.Assets} />
                )}
                pageSize={DEFAULT_PAGE_SIZE}
                emptyState={
                    canAddToAssets ? (
                        <EmptyContent
                            icon="Database"
                            title="No Assets"
                            description="Add assets to the parent entity to view them"
                            linkText="Add assets"
                            onLinkClick={() => setShowAddAssetsModal(true)}
                        />
                    ) : null
                }
                totalItemCount={total}
            />
            {showAddAssetsModal && <AddAssetsModal setShowAddAssetsModal={setShowAddAssetsModal} />}
        </LargeModule>
    );
}
