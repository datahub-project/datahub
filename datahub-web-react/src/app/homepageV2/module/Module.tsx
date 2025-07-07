import React, { useMemo } from 'react';

import SampleLargeModule from '@app/homepageV2/module/modules/SampleLargeModule';
import YourAssetsModule from '@app/homepageV2/module/modules/YourAssetsModule';
import { ModuleProps } from '@app/homepageV2/module/types';

import { DataHubPageModuleType } from '@types';

export default function Module(props: ModuleProps) {
    const { module } = props;
    const Component = useMemo(() => {
        if (module.properties.type === DataHubPageModuleType.OwnedAssets) return YourAssetsModule;
        // TODO: remove the sample large module once we have other modules to fill this out
        console.error(`Issue finding module with type ${module.properties.type}`);
        return SampleLargeModule;
    }, [module.properties.type]);

    return <Component module={module} />;
}
