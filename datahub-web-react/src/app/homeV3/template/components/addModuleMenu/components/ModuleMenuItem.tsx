import React from 'react';

import { ModuleInfo } from '@app/homeV3/modules/types';
import MenuItem from '@app/homeV3/template/components/addModuleMenu/components/MenuItem';

interface Props {
    module: ModuleInfo;
}

export default function ModuleMenuItem({ module }: Props) {
    return <MenuItem description={module.description} title={module.name} icon={module.icon} />;
}
