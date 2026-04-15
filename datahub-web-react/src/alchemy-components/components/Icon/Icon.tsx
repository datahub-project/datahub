import React, { useMemo } from 'react';

import { IconWrapper } from '@components/components/Icon/components';
import { IconProps, IconPropsDefaults } from '@components/components/Icon/types';
import { Tooltip } from '@components/components/Tooltip';
import { ColorOptions } from '@components/theme/config';
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

    const resolvedColor = useMemo(() => {
        const semantic = color ? theme?.colors?.[color as keyof typeof theme.colors] : undefined;
        return typeof semantic === 'string' ? semantic : getColor(color as ColorOptions, colorLevel, theme);
    }, [color, colorLevel, theme]);

    if (!IconComponent) return null;

    return (
        <IconWrapper size={getFontSize(size)} rotate={getRotationTransform(rotate)} {...props}>
            <Tooltip title={tooltipText}>
                <IconComponent style={{ fontSize: getFontSize(size), color: resolvedColor }} weight={weight} />
            </Tooltip>
        </IconWrapper>
    );
};
