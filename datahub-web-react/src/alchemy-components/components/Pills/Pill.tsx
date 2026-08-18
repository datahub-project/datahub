import { Icon } from '@components';
import React from 'react';

import { PillContainer, PillIconButton, PillIconSlot, PillText } from '@components/components/Pills/components';
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
    [PillVariantValues.squareFilled]: [
        ColorValues.primary,
        ColorValues.violet,
        ColorValues.blue,
        ColorValues.green,
        ColorValues.red,
        ColorValues.yellow,
        ColorValues.gray,
    ],
    [PillVariantValues.squareOutline]: [
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

export const Pill = React.forwardRef<HTMLDivElement, PillProps>(
    (
        {
            label,
            size = pillDefaults.size,
            variant = pillDefaults.variant,
            clickable = pillDefaults.clickable,
            color = pillDefaults.color,
            leftIcon,
            rightIcon,
            rightIcons,
            id,
            onClickRightIcon,
            onClickLeftIcon,
            onPillClick,
            customStyle,
            customIconRenderer,
            showLabel,
            className,
            dataTestId,
            // Explicitly forward the pointer/focus events that overlay components
            // (antd Popover/Tooltip) inject via cloneElement — otherwise their hover
            // and focus triggers silently no-op because Pill drops them on the floor.
            onMouseEnter,
            onMouseLeave,
            onFocus,
            onBlur,
            onPointerEnter,
            onPointerLeave,
        },
        ref,
    ) => {
        if (!SUPPORTED_CONFIGURATIONS[variant].includes(color)) {
            console.debug(`Unsupported configuration for Pill: variant=${variant}, color=${color}`);
        }

        const renderIcon = (
            icon: NonNullable<PillProps['leftIcon']>,
            onClick?: (e: React.MouseEvent<HTMLElement>) => void,
            ariaLabel?: string,
            testId?: string,
            key?: string,
        ) => {
            const iconNode = <Icon icon={icon} size={size} color="inherit" />;

            if (onClick) {
                return (
                    <PillIconButton
                        key={key}
                        type="button"
                        $size={size}
                        onClick={onClick}
                        aria-label={ariaLabel}
                        data-testid={testId}
                    >
                        {iconNode}
                    </PillIconButton>
                );
            }

            return (
                <PillIconSlot key={key} $size={size}>
                    {iconNode}
                </PillIconSlot>
            );
        };

        const hasLeftIcon = Boolean(customIconRenderer || leftIcon);
        const hasRightIcon = Boolean((rightIcons && rightIcons.length > 0) || rightIcon);

        return (
            <PillContainer
                ref={ref}
                variant={variant}
                color={color}
                size={size}
                clickable={clickable}
                $hasLeftIcon={hasLeftIcon}
                $hasRightIcon={hasRightIcon}
                id={id}
                data-testid={dataTestId ?? 'pill-container'}
                onClick={onPillClick}
                onMouseEnter={onMouseEnter}
                onMouseLeave={onMouseLeave}
                onFocus={onFocus}
                onBlur={onBlur}
                onPointerEnter={onPointerEnter}
                onPointerLeave={onPointerLeave}
                style={{
                    backgroundColor: customStyle?.backgroundColor,
                }}
                title={showLabel ? label : undefined}
                className={className}
            >
                {customIconRenderer ? customIconRenderer() : leftIcon && renderIcon(leftIcon, onClickLeftIcon)}
                <PillText style={customStyle}>{label}</PillText>
                {rightIcons && rightIcons.length > 0
                    ? rightIcons.map((r) =>
                          renderIcon(
                              r.icon,
                              r.onClick,
                              r.ariaLabel,
                              r.testId,
                              r.testId ?? r.ariaLabel ?? r.icon.displayName ?? r.icon.name,
                          ),
                      )
                    : rightIcon && renderIcon(rightIcon, onClickRightIcon)}
            </PillContainer>
        );
    },
);
