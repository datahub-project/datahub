import { borders, colors, spacing, transform, zIndices, radius } from '@components/theme';
import styled from 'styled-components';
import { getCheckboxColor, getCheckboxHoverBackgroundColor } from './utils';
import { formLabelTextStyles } from '../commonStyles';

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

export const Checkmark = styled.div<{ intermediate?: boolean; error: string; checked: boolean; disabled: boolean }>(
    ({ intermediate, checked, error, disabled }) => ({
        position: 'absolute',
        top: '4px',
        left: '11px',
        zIndex: zIndices.docked,
        height: '18px',
        width: '18px',
        borderRadius: '3px',
        border: `${borders['2px']} ${getCheckboxColor(checked, error, disabled, undefined)}`,
        transition: 'all 0.2s ease-in-out',
        cursor: 'pointer',
        '&:after': {
            content: '""',
            position: 'absolute',
            display: 'none',
            left: !intermediate ? '6px' : '8px',
            top: !intermediate ? '1px' : '3px',
            width: !intermediate ? '5px' : '0px',
            height: '10px',
            border: 'solid white',
            borderWidth: '0 3px 3px 0',
            transform: !intermediate ? 'rotate(45deg)' : transform.rotate[90],
        },
    }),
);

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
