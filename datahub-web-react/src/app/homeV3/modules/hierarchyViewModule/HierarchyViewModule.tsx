import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useHistory } from 'react-router';
import { v4 as uuidv4 } from 'uuid';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { ModuleProps } from '@app/homeV3/module/types';
import AssetsTreeView from '@app/homeV3/modules/hierarchyViewModule/components/AssetsTreeView';
import { ASSET_TYPE_DOMAINS, ASSET_TYPE_GLOSSARY } from '@app/homeV3/modules/hierarchyViewModule/constants';
import { filterAssetUrnsByAssetType, getAssetTypeFromAssetUrns } from '@app/homeV3/modules/hierarchyViewModule/utils';
import { useStableValue } from '@app/sharedV2/hooks/useStableValue';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';
import { convertLogicalPredicateToOrFilters } from '@app/sharedV2/queryBuilder/builder/utils';
import { PageRoutes } from '@conf/Global';

import { AndFilterInput } from '@types';

export default function HierarchyViewModule(props: ModuleProps) {
    const history = useHistory();
    const { showViewAll = true } = props;
    const { isReloading } = useModuleContext();

    const hierarchyViewParams = useStableValue(props.module.properties.params.hierarchyViewParams);

    // FIY: `hierarchyViewParamsJson` is used as key to force a re-mount of the AssetsTreeView component
    // whenever the `stableHierarchyViewParams` change
    const hierarchyViewParamsJson = useMemo(() => JSON.stringify(hierarchyViewParams), [hierarchyViewParams]);
    const [keySuffix, setSuffixKey] = useState<string>('');
    // Add generated suffix to key to re-mount module on reload (isReloading)
    useEffect(() => {
        if (isReloading) setSuffixKey(uuidv4());
    }, [isReloading]);
    const key = useMemo(() => `${hierarchyViewParamsJson}-${keySuffix}`, [hierarchyViewParamsJson, keySuffix]);

    const assetType = useMemo(() => getAssetTypeFromAssetUrns(hierarchyViewParams?.assetUrns), [hierarchyViewParams]);

    const assetUrns = useMemo(
        () => filterAssetUrnsByAssetType(hierarchyViewParams?.assetUrns, assetType),
        [hierarchyViewParams, assetType],
    );

    const shouldShowRelatedEntities = useMemo(
        () => !!hierarchyViewParams?.showRelatedEntities,
        [hierarchyViewParams?.showRelatedEntities],
    );

    const relatedEntitiesLogicalPredicate: LogicalPredicate | undefined = useMemo(
        () =>
            hierarchyViewParams?.relatedEntitiesFilterJson
                ? JSON.parse(hierarchyViewParams?.relatedEntitiesFilterJson)
                : undefined,
        [hierarchyViewParams?.relatedEntitiesFilterJson],
    );

    const relatedEntitiesOrFilters: AndFilterInput[] | undefined = useMemo(
        () =>
            relatedEntitiesLogicalPredicate
                ? convertLogicalPredicateToOrFilters(relatedEntitiesLogicalPredicate)
                : undefined,
        [relatedEntitiesLogicalPredicate],
    );

    const onClickViewAll = useCallback(() => {
        if (assetType === ASSET_TYPE_DOMAINS) {
            history.push(PageRoutes.DOMAINS);
        } else if (assetType === ASSET_TYPE_GLOSSARY) {
            history.push(PageRoutes.GLOSSARY);
        }
    }, [history, assetType]);

    return (
        <LargeModule {...props} onClickViewAll={showViewAll ? onClickViewAll : undefined} dataTestId="hierarchy-module">
            {assetUrns.length === 0 ? (
                <EmptyContent
                    icon="Stack"
                    title="No Assets"
                    description="Edit the module and add assets to see them in this list"
                />
            ) : (
                <AssetsTreeView
                    key={key}
                    assetType={assetType}
                    assetUrns={assetUrns}
                    shouldShowRelatedEntities={shouldShowRelatedEntities}
                    relatedEntitiesOrFilters={relatedEntitiesOrFilters}
                />
            )}
        </LargeModule>
    );
}
