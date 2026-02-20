import { Bar } from '@visx/shape';
import React from 'react';
import { useTheme } from 'styled-components';

type Props = {
    index: number;
    active: boolean;
    opacity: number;
};

const PopularityIconBar = ({ index, active, opacity }: Props) => {
    const theme = useTheme();
    const activeColor = theme.colors.textBrand;
    const inactiveColor = theme.colors.textDisabled;

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
            stroke={active ? activeColor : inactiveColor}
            fill={active ? activeColor : inactiveColor}
        />
    );
};

export default PopularityIconBar;
