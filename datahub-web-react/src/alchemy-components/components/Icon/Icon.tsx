import React from 'react';

import { getFontSize, getColor, getRotationTransform } from '@components/theme/utils';

import { IconProps } from './types';
import { IconWrapper } from './components';
import { getIconNames, getIconComponent } from './utils';

export const iconDefaults: IconProps = {
    icon: 'AccountCircle',
    variant: 'outline',
    size: '4xl',
    color: 'inherit',
    rotate: '0',
};

export const Icon = ({
    icon,
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
    const isOutlined = variant === 'outline';
    const outlinedIconName = `${icon}Outlined`;

    // Warn if the icon does not have the specified variant
    if (variant === 'outline' && !outlined.includes(outlinedIconName)) {
        console.warn(`Icon "${icon}" does not have an outlined variant.`);
        return null;
    }

    // Warn if the icon does not have the specified variant
    if (variant === 'filled' && !filled.includes(icon)) {
        console.warn(`Icon "${icon}" does not have a filled variant.`);
        return null;
    }

    // Get outlined icon component
    const IconComponent = getIconComponent(isOutlined ? outlinedIconName : icon);

    return (
        <IconWrapper size={getFontSize(size)} rotate={getRotationTransform(rotate)} {...props}>
            <IconComponent
                sx={{
                    fontSize: getFontSize(size),
                    color: getColor(color),
                }}
            />
        </IconWrapper>
    );
};
