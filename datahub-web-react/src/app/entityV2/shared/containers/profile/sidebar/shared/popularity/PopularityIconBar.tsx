/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Bar } from '@visx/shape';
import React from 'react';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';

const ACTIVE_COLOR = '#3F54D1';
const INACTIVE_COLOR = ANTD_GRAY[5];

type Props = {
    index: number;
    active: boolean;
    opacity: number;
};

const PopularityIconBar = ({ index, active, opacity }: Props) => {
    return (
        <Bar
            rx={2}
            ry={2}
            key={index}
            x={4 + 12 * index}
            y={14 - 6 * index}
            width={6}
            height={12 + index * 6}
            opacity={active ? opacity : 1}
            stroke={active ? ACTIVE_COLOR : INACTIVE_COLOR}
            fill={active ? ACTIVE_COLOR : INACTIVE_COLOR}
        />
    );
};

export default PopularityIconBar;
