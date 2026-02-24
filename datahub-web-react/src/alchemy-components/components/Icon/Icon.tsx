import { Tooltip } from '@components';
import React from 'react';

import { IconWrapper } from '@components/components/Icon/components';
import { IconProps, IconPropsDefaults } from '@components/components/Icon/types';
import { getColor, getFontSize, getRotationTransform } from '@components/theme/utils';

import { useCustomTheme } from '@src/customThemeContext';

export const iconDefaults: IconPropsDefaults = {
    size: '4xl',
    color: 'inherit',
    rotate: '0',
    tooltipText: '',
};

export const Icon = ({
    icon: IconComponent,
    size = iconDefaults.size,
    color = iconDefaults.color,
    colorLevel,
    rotate = iconDefaults.rotate,
    weight,
    tooltipText,
    ...props
}: IconProps) => {
    const { theme } = useCustomTheme();

    if (!IconComponent) return null;

    return (
        <IconWrapper size={getFontSize(size)} rotate={getRotationTransform(rotate)} {...props}>
            <Tooltip title={tooltipText}>
                <IconComponent
                    style={{ fontSize: getFontSize(size), color: getColor(color, colorLevel, theme) }}
                    weight={weight}
                />
            </Tooltip>
        </IconWrapper>
    );
};
