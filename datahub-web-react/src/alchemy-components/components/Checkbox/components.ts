import styled from 'styled-components';

import { getCheckboxBorder, getCheckboxSize, getCheckboxTouchTarget } from '@components/components/Checkbox/utils';
import { formLabelTextStyles } from '@components/components/commonStyles';
import { radius, spacing } from '@components/theme';

import { SizeOptions } from '@src/alchemy-components/theme/config';

export const CheckboxContainer = styled.div<{ justifyContent?: 'center' | 'flex-start' | undefined; gap?: string }>(
    ({ justifyContent, gap }) => ({
        display: 'flex',
        justifyContent: justifyContent ?? 'center',
        alignItems: 'center',
        gap,
        padding: '0 4px',
    }),
);

export const Label = styled.div<{ clickable?: boolean }>(({ clickable }) => ({
    ...formLabelTextStyles,
    ...(clickable ? { cursor: 'pointer' } : {}),
}));

export const Required = styled.span`
    color: ${(props) => props.theme?.colors?.textError};
    margin-left: ${spacing.xxsm};
`;

export const CheckboxBase = styled.div<{ $size: SizeOptions }>`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    min-width: ${(props) => getCheckboxTouchTarget(props.$size)};
    min-height: ${(props) => getCheckboxTouchTarget(props.$size)};

    &[data-disabled='true'] {
        cursor: not-allowed;
    }
`;

export const HiddenInput = styled.input`
    position: absolute;
    opacity: 0;
    width: 0;
    height: 0;
    pointer-events: none;
`;

export const CheckboxBox = styled.div<{
    $checked: boolean;
    $error: boolean;
    $disabled: boolean;
    $size: SizeOptions;
}>`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: ${(props) => getCheckboxSize(props.$size)};
    height: ${(props) => getCheckboxSize(props.$size)};
    border-radius: ${radius.sm};
    border: ${(props) => getCheckboxBorder(props.$checked, props.$error, props.$disabled, props.theme?.colors)};
    background: ${(props) => {
        if (props.$disabled && props.$checked) return props.theme?.colors?.bgDisabled;
        if (props.$disabled) return props.theme?.colors?.bgSurfaceDisabled;
        if (props.$checked) return props.theme?.colors?.brandGradient;
        return props.theme?.colors?.bg;
    }};
    transition: all 0.15s ease-in-out;
    cursor: inherit;

    svg {
        color: ${(props) => {
            if (props.$disabled) return props.theme?.colors?.iconDisabled;
            return props.theme?.colors?.textOnFillBrand;
        }};
    }
`;

export const CheckboxGroupContainer = styled.div<{ isVertical?: boolean }>(({ isVertical }) => ({
    display: 'flex',
    flexDirection: isVertical ? 'column' : 'row',
    justifyContent: 'center',
    alignItems: 'center',
    gap: spacing.md,
    margin: spacing.xxsm,
}));
