import React from 'react';

import { IconWrapper } from '@components/components/Icon/components';
import { IconProps, IconPropsDefaults } from '@components/components/Icon/types';
import { getIconComponent, getIconNames } from '@components/components/Icon/utils';
import { getColor, getFontSize, getRotationTransform } from '@components/theme/utils';

export const iconDefaults: IconPropsDefaults = {
    source: 'material',
    variant: 'outline',
    size: '4xl',
    color: 'inherit',
    rotate: '0',
};

export const Icon = ({
    icon,
    source = iconDefaults.source,
    variant = iconDefaults.variant,
    size = iconDefaults.size,
    color = iconDefaults.color,
    rotate = iconDefaults.rotate,
    ...props
}: IconProps) => {
    const { filled, outlined } = getIconNames();

    // Return early if no icon is provided
    if (!icon) return null;

    // Get outlined icon component name
    const iconName = source === 'material' && variant === 'outline' ? `${icon}Outlined` : icon;

    // Warn if the icon does not have the specified variant
    if (source === 'material' && variant === 'outline' && !outlined.includes(iconName)) {
        console.warn(`Icon "${icon}" does not have an outlined variant.`);
        return null;
    }

    // Warn if the icon does not have the specified variant
    if (source === 'material' && variant === 'filled' && !filled.includes(iconName)) {
        console.warn(`Icon "${icon}" does not have a filled variant.`);
        return null;
    }

    const IconComponent = getIconComponent(source, iconName);

    return (
        <IconWrapper size={getFontSize(size)} rotate={getRotationTransform(rotate)} {...props}>
            <IconComponent
                sx={{
                    fontSize: getFontSize(size),
                    color: getColor(color),
                }}
                style={{ color: getColor(color) }}
            />
        </IconWrapper>
    );
};
