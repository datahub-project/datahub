import React, { memo, useMemo } from 'react';

import { ModuleProps } from '@app/homeV3/module/types';
import SampleLargeModule from '@app/homeV3/modules/SampleLargeModule';
import YourAssetsModule from '@app/homeV3/modules/YourAssetsModule';
import AssetCollectionModule from '@app/homeV3/modules/assetCollection/AssetCollectionModule';
import TopDomainsModule from '@app/homeV3/modules/domains/TopDomainsModule';
import LinkModule from '@app/homeV3/modules/link/LinkModule';

import { DataHubPageModuleType } from '@types';

function Module(props: ModuleProps) {
    const { module } = props;

    // Memoize component selection to prevent re-evaluation on every render
    const Component = useMemo(() => {
        if (module.properties.type === DataHubPageModuleType.OwnedAssets) return YourAssetsModule;
        if (module.properties.type === DataHubPageModuleType.Domains) return TopDomainsModule;
        if (module.properties.type === DataHubPageModuleType.AssetCollection) return AssetCollectionModule;
        if (module.properties.type === DataHubPageModuleType.Link) return LinkModule;

        // TODO: remove the sample large module once we have other modules to fill this out
        console.error(`Issue finding module with type ${module.properties.type}`);
        return SampleLargeModule;
    }, [module.properties.type]);

    return <Component {...props} />;
}

// Export memoized component to prevent unnecessary re-renders
export default memo(Module);
