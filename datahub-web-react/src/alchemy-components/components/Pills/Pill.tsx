import { Icon } from '@components';
import React from 'react';

import { PillContainer } from './components';
import { PillProps } from './types';

export const pillDefault: PillProps = {
    label: 'Label',
    size: 'md',
    variant: 'filled',
    clickable: true,
};

export function Pill({
    label = pillDefault.label,
    size = pillDefault.size,
    leftIcon,
    rightIcon,
    colorScheme,
    variant = pillDefault.variant,
    clickable = pillDefault.clickable,
    id,
    onClickRightIcon,
    onClickLeftIcon,
}: PillProps) {
    return (
        <PillContainer
            variant={variant}
            colorScheme={colorScheme}
            size={size}
            clickable={clickable}
            id={id}
            data-testid="pill-container"
        >
            {leftIcon && <Icon icon={leftIcon} size={size} onClick={onClickLeftIcon} />}
            {label}
            {rightIcon && <Icon icon={rightIcon} size={size} onClick={onClickRightIcon} />}
        </PillContainer>
    );
}
