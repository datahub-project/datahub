import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useHistory } from 'react-router';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';
import AssetsTreeView from '@app/homeV3/modules/hierarchyViewModule/components/AssetsTreeView';
import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from '@app/homeV3/modules/hierarchyViewModule/constants';
import { filterAssetUrnsByAssetType, getAssetTypeFromAssetUrns } from '@app/homeV3/modules/hierarchyViewModule/utils';
import { PageRoutes } from '@conf/Global';

export default function HierarchyViewModule(props: ModuleProps) {
    const history = useHistory();

    // Run force rerendering of tree to reinitialize its state correctly
    // TODO: is there are a better solution?
    // ----------------------------------------------------------------
    const [shouldShowTree, setShodShowTree] = useState<boolean>(true);

    useEffect(() => {
        setShodShowTree(false);
    }, [props.module.properties.params.hierarchyViewParams]);

    useEffect(() => {
        if (!shouldShowTree) setShodShowTree(true);
    }, [shouldShowTree]);
    // ----------------------------------------------------------------

    const assetType = useMemo(
        () => getAssetTypeFromAssetUrns(props.module.properties.params.hierarchyViewParams?.assetUrns),
        [props.module],
    );

    const assetUrns = useMemo(
        () => filterAssetUrnsByAssetType(props.module.properties.params.hierarchyViewParams?.assetUrns, assetType),
        [props.module, assetType],
    );

    const shouldShowRelatedEntities = useMemo(
        () => !!props.module.properties.params.hierarchyViewParams?.showRelatedEntities,
        [props.module.properties.params.hierarchyViewParams?.showRelatedEntities],
    );

    const onClickViewAll = useCallback(() => {
        if (assetType === ASSET_TYPE_DOMAINS) {
            history.push(PageRoutes.DOMAINS);
        } else if (assetType === ASSET_TYPE_GLOSSARY) {
            history.push(PageRoutes.GLOSSARY);
        }
    }, [history, assetType]);

    return (
        <LargeModule {...props} onClickViewAll={onClickViewAll}>
            {assetUrns.length === 0 ? (
                <EmptyContent
                    icon="Stack"
                    title="No Assets"
                    description="Edit the module and add assets to see them in this list"
                />
            ) : (
                shouldShowTree && (
                    <AssetsTreeView
                        assetType={assetType}
                        assetUrns={assetUrns}
                        shouldShowRelatedEntities={shouldShowRelatedEntities}
                    />
                )
            )}
        </LargeModule>
    );
}
