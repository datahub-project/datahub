import React, { useMemo } from 'react';

import EmptyContent from '@app/homeV3/module/components/EmptyContent';
import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

import { filterAssetUrnsByAssetType, getAssetTypeFromAssetUrns } from './utils';
import DomainsTreeView from './module/domainsTreeView/DomainsTreeView';
import AssetsTreeView from './module/domainsTreeView/AssetsTreeView';

export default function HierarchyViewModule(props: ModuleProps) {
    console.log('>>>MODULE props', props);
    console.log('>>>MODULE NAME', props.module.properties.name);
    console.log('>>>MODULE ORIGIN assetUrns', props.module.properties.params.hierarchyViewParams?.assetUrns);

    const assetType = useMemo(
        () => getAssetTypeFromAssetUrns(props.module.properties.params.hierarchyViewParams?.assetUrns),
        [props.module.properties.params.hierarchyViewParams?.assetUrns],
    );
    console.log('>>>MODULE assetType', assetType);

    const assetUrns = useMemo(
        () => filterAssetUrnsByAssetType(props.module.properties.params.hierarchyViewParams?.assetUrns, assetType),
        [props.module.properties.params.hierarchyViewParams?.assetUrns, assetType],
    );
    console.log('>>>MODULE assetUrns', assetUrns);

    return (
        <LargeModule {...props}>
            {/* <EmptyContent
                icon="Stack"
                title="No Assets"
                description="Edit the module and add assets to see them in this list"
            /> */}

            <AssetsTreeView assetType={assetType} assetUrns={assetUrns}/>
        </LargeModule>
    );
}
