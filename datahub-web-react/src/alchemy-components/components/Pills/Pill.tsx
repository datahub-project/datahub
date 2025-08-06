import { Button, Icon } from '@components';
import React from 'react';

import { PillContainer, PillText } from '@components/components/Pills/components';
import { PillProps, PillPropsDefaults } from '@components/components/Pills/types';
import { ColorOptions, ColorValues, PillVariantOptions, PillVariantValues, SizeValues } from '@components/theme/config';

export const SUPPORTED_CONFIGURATIONS: Record<PillVariantOptions, ColorOptions[]> = {
    [PillVariantValues.filled]: [
        ColorValues.primary,
        ColorValues.violet,
        ColorValues.blue,
        ColorValues.green,
        ColorValues.red,
        ColorValues.yellow,
        ColorValues.gray,
    ],
    [PillVariantValues.outline]: [
        ColorValues.primary,
        ColorValues.violet,
        ColorValues.blue,
        ColorValues.green,
        ColorValues.red,
        ColorValues.yellow,
        ColorValues.gray,
    ],
    [PillVariantValues.version]: [ColorValues.white, ColorValues.gray],
};

export const pillDefaults: PillPropsDefaults = {
    size: SizeValues.md,
    variant: PillVariantValues.filled,
    color: ColorValues.gray,
    clickable: false,
};

export function Pill({
    label,
    size = pillDefaults.size,
    variant = pillDefaults.variant,
    clickable = pillDefaults.clickable,
    color = pillDefaults.color,
    leftIcon,
    rightIcon,
    id,
    onClickRightIcon,
    onClickLeftIcon,
    onPillClick,
    customStyle,
    customIconRenderer,
    showLabel,
    className,
}: PillProps) {
    if (!SUPPORTED_CONFIGURATIONS[variant].includes(color)) {
        console.debug(`Unsupported configuration for Pill: variant=${variant}, color=${color}`);
    }

    return (
        <PillContainer
            variant={variant}
            color={color}
            size={size}
            clickable={clickable}
            id={id}
            data-testid="pill-container"
            onClick={onPillClick}
            style={{
                backgroundColor: customStyle?.backgroundColor,
            }}
            title={showLabel ? label : undefined}
            className={className}
        >
            {customIconRenderer
                ? customIconRenderer()
                : leftIcon && <Icon icon={leftIcon} size={size} onClick={onClickLeftIcon} />}
            <PillText style={customStyle}>{label}</PillText>
            {rightIcon && (
                <Button style={{ padding: 0 }} variant="text" onClick={onClickRightIcon}>
                    <Icon icon={rightIcon} size={size} />
                </Button>
            )}
        </PillContainer>
    );
}
