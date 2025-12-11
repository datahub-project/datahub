/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Group } from '@visx/group';
import React from 'react';

import PopularityIconBar from '@app/entityV2/shared/containers/profile/sidebar/shared/popularity/PopularityIconBar';
import { PopularityTier } from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';

type Props = {
    tier: PopularityTier;
    width?: number;
    height?: number;
};

const PopularityIcon = ({ tier, width = 38, height = 32 }: Props) => {
    return (
        <svg width={width} height={height}>
            <Group>
                <PopularityIconBar index={0} active={tier < 3} opacity={0.6} />
                <PopularityIconBar index={1} active={tier < 2} opacity={0.8} />
                <PopularityIconBar index={2} active={tier < 1} opacity={1} />
            </Group>
        </svg>
    );
};

export default PopularityIcon;
