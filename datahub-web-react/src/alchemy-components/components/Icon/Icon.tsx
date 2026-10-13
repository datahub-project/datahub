import React, { forwardRef, useMemo } from 'react';

import { IconWrapper } from '@components/components/Icon/components';
import { IconProps, IconPropsDefaults } from '@components/components/Icon/types';
import { Tooltip } from '@components/components/Tooltip';
import { getFontSize, getRotationTransform, getThemedIconColor } from '@components/theme/utils';

import { useCustomTheme } from '@src/customThemeContext';

export const iconDefaults: IconPropsDefaults = {
    size: '4xl',
    color: 'inherit',
    rotate: '0',
    tooltipText: '',
};

// Forwards the ref so overlays such as `Tooltip` can anchor to the wrapper when an Icon is their
// trigger; without it the overlay has no reference element and renders unpositioned.
export const Icon = forwardRef<HTMLDivElement, IconProps>(
    (
        {
            icon: IconComponent,
            size = iconDefaults.size,
            color = iconDefaults.color,
            colorLevel,
            rotate = iconDefaults.rotate,
            weight,
            tooltipText,
            ...props
        },
        ref,
    ) => {
        const { theme } = useCustomTheme();

        const resolvedColor = useMemo(() => getThemedIconColor(color, colorLevel, theme), [color, colorLevel, theme]);

        if (!IconComponent) return null;

        return (
            <IconWrapper ref={ref} size={getFontSize(size)} rotate={getRotationTransform(rotate)} {...props}>
                <Tooltip title={tooltipText}>
                    <IconComponent style={{ fontSize: getFontSize(size), color: resolvedColor }} weight={weight} />
                </Tooltip>
            </IconWrapper>
        );
    },
);

Icon.displayName = 'Icon';
