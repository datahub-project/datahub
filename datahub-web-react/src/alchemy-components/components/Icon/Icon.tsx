import { Tooltip } from '@components';
import React from 'react';

import { IconWrapper } from '@components/components/Icon/components';
import { IconProps, IconPropsDefaults } from '@components/components/Icon/types';
import { getIconComponent } from '@components/components/Icon/utils';
import { getColor, getFontSize, getRotationTransform } from '@components/theme/utils';

import { useCustomTheme } from '@src/customThemeContext';

export const iconDefaults: IconPropsDefaults = {
    size: 'lg',
    color: 'inherit',
    rotate: '0',
    tooltipText: '',
};

export const Icon = React.forwardRef<HTMLSpanElement, IconProps>(
    (
        {
            icon,
            size = iconDefaults.size,
            color = iconDefaults.color,
            colorLevel,
            rotate = iconDefaults.rotate,
            weight,
            tooltipText,
            source: _source,
            variant: _variant,
            ...props
        },
        ref,
    ) => {
        const { theme } = useCustomTheme();

        if (!icon) return null;

        const IconComponent = getIconComponent(icon);

        if (!IconComponent) {
            console.warn(`Unknown icon: ${icon}`);
            return null;
        }

        const fontSizeStr = getFontSize(size);
        const resolvedSize = fontSizeStr === 'inherit' ? undefined : parseInt(fontSizeStr, 10) || undefined;

        return (
            <IconWrapper ref={ref} rotate={getRotationTransform(rotate)} {...props}>
                <Tooltip title={tooltipText}>
                    <IconComponent
                        size={resolvedSize}
                        style={{ color: getColor(color, colorLevel, theme) }}
                        weight={weight}
                    />
                </Tooltip>
            </IconWrapper>
        );
    },
);

Icon.displayName = 'Icon';
