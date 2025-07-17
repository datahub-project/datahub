import React, { useMemo } from 'react';

import { ModuleProps } from '@app/homeV3/module/types';
import SampleLargeModule from '@app/homeV3/modules/SampleLargeModule';
import YourAssetsModule from '@app/homeV3/modules/YourAssetsModule';
import AssetCollectionModule from '@app/homeV3/modules/assetCollection/AssetCollectionModule';
import TopDomainsModule from '@app/homeV3/modules/domains/TopDomainsModule';
import HierarchyViewModule from '@app/homeV3/modules/hierarchyViewModule/HierarchyViewModule';

import { DataHubPageModuleType } from '@types';

export default function Module(props: ModuleProps) {
    const { module } = props;
    const Component = useMemo(() => {
        if (module.properties.type === DataHubPageModuleType.OwnedAssets) return YourAssetsModule;
        if (module.properties.type === DataHubPageModuleType.Domains) return TopDomainsModule;
        if (module.properties.type === DataHubPageModuleType.AssetCollection) return AssetCollectionModule;
        if (module.properties.type === DataHubPageModuleType.Hierarchy) return HierarchyViewModule;

        // TODO: remove the sample large module once we have other modules to fill this out
        console.error(`Issue finding module with type ${module.properties.type}`);
        return SampleLargeModule;
    }, [module.properties.type]);

    return <Component module={module} />;
}
