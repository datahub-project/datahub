import React, { useMemo } from 'react';

import { ModuleProps } from '@app/homeV3/module/types';
import SampleLargeModule from '@app/homeV3/modules/SampleLargeModule';
import YourAssetsModule from '@app/homeV3/modules/YourAssetsModule';

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
