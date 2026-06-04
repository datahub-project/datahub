import { InfiniteScrollList } from '@components';
import { Database } from '@phosphor-icons/react/dist/csr/Database';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';

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
    const { t } = useTranslation('modules');
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
                            icon={Database}
                            title={t('assets.emptyTitle')}
                            description={t('assets.emptyDescription')}
                            linkText={t('assets.emptyLink')}
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
