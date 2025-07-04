import React, { useMemo } from 'react';

import { ModuleProps } from '@app/homeV3/module/types';
import SampleLargeModule from '@app/homeV3/modules/SampleLargeModule';
import YourAssetsModule from '@app/homeV3/modules/YourAssetsModule';

export default function Module(props: ModuleProps) {
    const Component = useMemo(() => {
        // TODO: implement logic to map props.type to component
        if (props.type === 'sampleLarge') return SampleLargeModule;
        if (props.type === 'yourAssets') return YourAssetsModule;
        return SampleLargeModule;
    }, [props.type]);

    return <Component {...props} />;
}
