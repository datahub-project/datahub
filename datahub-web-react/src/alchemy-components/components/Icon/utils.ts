/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as materialIcons from '@mui/icons-material';
import * as phosphorIcons from '@phosphor-icons/react';

import { IconSource } from '@components/components/Icon/types';

export const getIconNames = () => {
    // We only want "Filled" (mui default) and "Outlined" icons
    const filtered = Object.keys(materialIcons).filter(
        (key) =>
            !key.includes('Filled') && !key.includes('TwoTone') && !key.includes('Rounded') && !key.includes('Sharp'),
    );

    const filled: string[] = [];
    const outlined: string[] = [];

    filtered.forEach((key) => {
        if (key.includes('Outlined')) {
            outlined.push(key);
        } else if (!key.includes('Outlined')) {
            filled.push(key);
        }
    });

    return {
        filled,
        outlined,
    };
};

export const getIconComponent = (source: IconSource, icon: string) => {
    return source === 'phosphor' ? phosphorIcons[icon] : materialIcons[icon];
};
