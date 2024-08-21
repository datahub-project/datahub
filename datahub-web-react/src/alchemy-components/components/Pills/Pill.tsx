import React from 'react';
import { Icon } from '@components';

import { PillContainer } from './components';
import { PillProps } from './types';

export const pillDefault: PillProps = {
    label: 'Label',
    size: 'md',
    variant: 'filled',
};

export function Pill({
    label = pillDefault.label,
    size = pillDefault.size,
    leftIcon,
    rightIcon,
    colorScheme,
    variant = pillDefault.variant,
}: PillProps) {
    return (
        <PillContainer variant={variant} colorScheme={colorScheme} size={size}>
            {leftIcon && <Icon icon={leftIcon} size={size} />}
            {label}
            {rightIcon && <Icon icon={rightIcon} size={size} />}
        </PillContainer>
    );
}
