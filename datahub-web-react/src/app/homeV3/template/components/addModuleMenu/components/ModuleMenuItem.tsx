/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { ModuleInfo } from '@app/homeV3/modules/types';
import MenuItem from '@app/homeV3/template/components/addModuleMenu/components/MenuItem';

interface Props {
    module: ModuleInfo;
    isDisabled?: boolean;
    isSmallModule?: boolean;
}

export default function ModuleMenuItem({ module, isDisabled, isSmallModule }: Props) {
    return (
        <MenuItem
            description={module.description}
            title={module.name}
            icon={module.icon}
            isDisabled={isDisabled}
            isSmallModule={isSmallModule}
        />
    );
}
