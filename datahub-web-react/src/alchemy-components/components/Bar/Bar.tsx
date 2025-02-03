import React from 'react';
import { colors } from '@src/alchemy-components/theme';

import { BarComponentProps } from './types';
import { BarContainer, IndividualBar } from './components';
import { BAR_HEIGHT_MULTIPLIER } from './constant';

const defaultProps: BarComponentProps = {
    color: colors.violet[500],
    coloredBars: 2,
    size: 'default',
};
export const Bar = ({
    color = defaultProps.color,
    coloredBars = defaultProps.coloredBars,
    size = defaultProps.size,
}: BarComponentProps) => {
    const Bars = Array.from({ length: 3 }, (_, index) => {
        const barHeight = (index + 2) * BAR_HEIGHT_MULTIPLIER[size];
        return (
            <IndividualBar key={index} size={size} height={barHeight} isColored={index < coloredBars} color={color} />
        );
    });
    return <BarContainer>{Bars}</BarContainer>;
};
