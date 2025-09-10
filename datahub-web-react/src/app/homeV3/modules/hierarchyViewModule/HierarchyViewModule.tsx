import React, { useCallback, useMemo } from 'react';
import { useHistory } from 'react-router';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
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

    const hierarchyViewParams = useStableValue(props.module.properties.params.hierarchyViewParams);

    // FIY: `hierarchyViewParamsJson` is used as key to force a re-mount of the AssetsTreeView component
    // whenever the `stableHierarchyViewParams` change
    const hierarchyViewParamsJson = useMemo(() => JSON.stringify(hierarchyViewParams), [hierarchyViewParams]);

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
                    key={hierarchyViewParamsJson}
                    assetType={assetType}
                    assetUrns={assetUrns}
                    shouldShowRelatedEntities={shouldShowRelatedEntities}
                    relatedEntitiesOrFilters={relatedEntitiesOrFilters}
                />
            )}
        </LargeModule>
    );
}
