import React, { useMemo } from 'react';

import MenuItem from '@app/homeV3/template/components/addModuleMenu/components/MenuItem';
import {
    getModuleDescription,
    getModuleIcon,
    getModuleTitle,
} from '@app/homeV3/template/components/addModuleMenu/utils';

import { DataHubPageModule } from '@types';

interface Props {
    module: DataHubPageModule;
}

export default function ModuleMenuItem({ module }: Props) {
    const icon = useMemo(() => getModuleIcon(module), [module]);
    const title = useMemo(() => getModuleTitle(module), [module]);
    const description = useMemo(() => getModuleDescription(module), [module]);

    return <MenuItem description={description} title={title} icon={icon} />;
}
