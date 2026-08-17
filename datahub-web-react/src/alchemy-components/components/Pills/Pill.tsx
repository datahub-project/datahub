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

        return (
            <PillContainer
                ref={ref}
                variant={variant}
                color={color}
                size={size}
                clickable={clickable}
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
                {customIconRenderer
                    ? customIconRenderer()
                    : leftIcon && <Icon icon={leftIcon} size={size} onClick={onClickLeftIcon} />}
                <PillText style={customStyle}>{label}</PillText>
                {rightIcons && rightIcons.length > 0
                    ? rightIcons.map((r, idx) => (
                          <Button
                              // eslint-disable-next-line react/no-array-index-key
                              key={idx}
                              style={{ padding: 0 }}
                              variant="text"
                              color={color}
                              onClick={r.onClick}
                              aria-label={r.ariaLabel}
                              data-testid={r.testId}
                          >
                              <Icon icon={r.icon} size={size} />
                          </Button>
                      ))
                    : rightIcon && (
                          <Button style={{ padding: 0 }} variant="text" color={color} onClick={onClickRightIcon}>
                              <Icon icon={rightIcon} size={size} />
                          </Button>
                      )}
            </PillContainer>
        );
    },
);
