import { Icon } from '@components';
import React from 'react';
import { PillContainer, PillText } from './components';
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
    onPillClick,
    customStyle,
    customIconRenderer,
}: PillProps) {
    return (
        <PillContainer
            variant={variant}
            colorScheme={colorScheme}
            size={size}
            clickable={clickable}
            id={id}
            data-testid="pill-container"
            onClick={onPillClick}
            style={{
                backgroundColor: customStyle?.backgroundColor,
            }}
        >
            {customIconRenderer
                ? customIconRenderer()
                : leftIcon && <Icon icon={leftIcon} size={size} onClick={onClickLeftIcon} />}
            <PillText style={customStyle}>{label}</PillText>
            {rightIcon && <Icon icon={rightIcon} size={size} onClick={onClickRightIcon} />}
        </PillContainer>
    );
}
