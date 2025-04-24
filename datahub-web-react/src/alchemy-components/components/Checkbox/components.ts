import styled from 'styled-components';

import {
    getCheckboxColor,
    getCheckboxHoverBackgroundColor,
    getCheckboxSize,
    getCheckmarkPosition,
} from '@components/components/Checkbox/utils';
import { formLabelTextStyles } from '@components/components/commonStyles';
import { borders, colors, radius, spacing, transform, zIndices } from '@components/theme';

import { SizeOptions } from '@src/alchemy-components/theme/config';

export const CheckboxContainer = styled.div({
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
});

export const Label = styled.div({
    ...formLabelTextStyles,
});

export const Required = styled.span({
    color: colors.red[500],
    marginLeft: spacing.xxsm,
});

export const CheckboxBase = styled.div({
    position: 'relative',
    width: '30px',
    height: '30px',
});

export const StyledCheckbox = styled.input<{
    checked: boolean;
    error: string;
    disabled: boolean;
}>(({ error, checked, disabled }) => ({
    position: 'absolute',
    opacity: 0,
    height: 0,
    width: 0,
    '&:checked + div': {
        backgroundColor: getCheckboxColor(checked, error, disabled, 'background'),
    },
    '&:checked + div:after': {
        display: 'block',
    },
}));

export const Checkmark = styled.div<{
    intermediate?: boolean;
    error: string;
    checked: boolean;
    disabled: boolean;
    size: SizeOptions;
}>(({ intermediate, checked, error, disabled, size }) => ({
    ...getCheckboxSize(size),
    ...getCheckmarkPosition(size),
    position: 'absolute',
    zIndex: zIndices.docked,
    borderRadius: '4px',
    border: `${borders['1px']} ${getCheckboxColor(checked, error, disabled, undefined)}`,
    transition: 'all 0.2s ease-in-out',
    cursor: disabled ? 'normal' : 'pointer',
    ':hover': {
        ...(!disabled && {
            borderColor: colors.violet[500],
        }),
    },
    '&:after': {
        content: '""',
        position: 'absolute',
        display: 'none',
        top: !intermediate ? '10%' : '25%',
        left: !intermediate ? '30%' : '45%',
        width: !intermediate ? '35%' : '0px',
        height: !intermediate ? '60%' : '50%',
        border: 'solid white',
        borderWidth: '0 2px 2px 0',
        transform: !intermediate ? 'rotate(45deg)' : transform.rotate[90],
    },
    ...(disabled && {
        backgroundColor: colors.gray[1500],
    }),
}));

export const HoverState = styled.div<{ isHovering: boolean; error: string; checked: boolean; disabled: boolean }>(
    ({ isHovering, error, checked }) => ({
        width: '40px',
        height: '40px',
        backgroundColor: !isHovering ? 'transparent' : getCheckboxHoverBackgroundColor(checked, error),
        position: 'absolute',
        borderRadius: radius.full,
        top: '-5px',
        left: '2px',
        zIndex: zIndices.hide,
    }),
);

export const CheckboxGroupContainer = styled.div<{ isVertical?: boolean }>(({ isVertical }) => ({
    display: 'flex',
    flexDirection: isVertical ? 'column' : 'row',
    justifyContent: 'center',
    alignItems: 'center',
    gap: spacing.md,
    margin: spacing.xxsm,
}));
