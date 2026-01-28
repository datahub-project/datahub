import React from 'react';

import LargeModule from '@app/homeV3/module/components/LargeModule';
import { ModuleProps } from '@app/homeV3/module/types';

export default function SampleLargeModule(props: ModuleProps) {
    return <LargeModule {...props}>Content of the sample module</LargeModule>;
}
