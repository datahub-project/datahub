import React, { useMemo } from 'react';

import SampleLargeModule from '@app/homepageV2/module/modules/SampleLargeModule';
import { ModuleProps } from '@app/homepageV2/module/types';

export default function Module(props: ModuleProps) {
    const Component = useMemo(() => {
        // TODO: implement logic to map props.type to component
        if (props.type === 'sampleLarge') return SampleLargeModule;
        return SampleLargeModule;
    }, [props.type]);

    return <Component {...props} />;
}
