/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { BarContainer, IndividualBar } from '@components/components/Bar/components';
import { BAR_HEIGHT_MULTIPLIER } from '@components/components/Bar/constant';
import { BarComponentProps } from '@components/components/Bar/types';

import { colors } from '@src/alchemy-components/theme';

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
