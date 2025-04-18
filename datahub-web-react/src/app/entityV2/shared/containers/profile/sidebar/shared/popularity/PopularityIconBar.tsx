import { Bar } from '@visx/shape';
import React from 'react';
import { ANTD_GRAY } from '../../../../../constants';

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
