import React from 'react';
import { useTheme } from 'styled-components';

import { BarContainer, IndividualBar } from '@components/components/Bar/components';
import { BAR_HEIGHT_MULTIPLIER } from '@components/components/Bar/constant';
import { BarComponentProps } from '@components/components/Bar/types';

export const Bar = ({ color, coloredBars = 2, size = 'default' }: BarComponentProps) => {
    const theme = useTheme();
    const resolvedColor = color ?? theme.colors.chartsBrandMedium;
    const Bars = Array.from({ length: 3 }, (_, index) => {
        const barHeight = (index + 2) * BAR_HEIGHT_MULTIPLIER[size];
        return (
            <IndividualBar
                key={index}
                size={size}
                height={barHeight}
                isColored={index < coloredBars}
                color={resolvedColor}
            />
        );
    });
    return <BarContainer>{Bars}</BarContainer>;
};
