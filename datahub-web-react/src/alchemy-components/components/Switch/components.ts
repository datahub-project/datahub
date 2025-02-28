import styled from 'styled-components';

import { borders, colors, shadows, spacing, transition } from '@components/theme';
import { ColorOptions, SizeOptions } from '@components/theme/config';

import { Icon } from '../Icon';

import { formLabelTextStyles } from '../commonStyles';

import {
    getIconTransformPositionLeft,
    getIconTransformPositionTop,
    getInputHeight,
    getSliderTransformPosition,
    getToggleSize,
} from './utils';

import type { SwitchLabelPosition } from './types';

export const Label = styled.div({
    ...formLabelTextStyles,
    display: 'flex',
    alignItems: 'flex-start',
});

export const SwitchContainer = styled.label<{ labelPosition: SwitchLabelPosition; isDisabled?: boolean }>(
    ({ labelPosition, isDisabled }) => ({
        display: 'flex',
        flexDirection: labelPosition === 'top' ? 'column' : 'row',
        alignItems: labelPosition === 'top' ? 'flex-start' : 'center',
        gap: spacing.sm,
        cursor: isDisabled ? 'not-allowed' : 'pointer',
        width: 'max-content',
    }),
);

export const Slider = styled.div<{ size?: SizeOptions; isSquare?: boolean; isDisabled?: boolean }>(
    ({ size, isSquare, isDisabled }) => ({
        '&:before': {
            transition: `${transition.duration.normal} all`,
            content: '""',
            position: 'absolute',
            minWidth: getToggleSize(size || 'md', 'slider'), // sliders width and height must be same
            minHeight: getToggleSize(size || 'md', 'slider'),
            borderRadius: !isSquare ? '35px' : '0px',
            top: '50%',
            left: spacing.xxsm,
            transform: 'translate(0, -50%)',
            backgroundColor: !isDisabled ? colors.white : colors.gray[200],
            boxShadow: `
				0px 1px 2px 0px rgba(16, 24, 40, 0.06),
				0px 1px 3px 0px rgba(16, 24, 40, 0.12)
			`,
        },
        borderRadius: !isSquare ? '32px' : '0px',
        minWidth: getToggleSize(size || 'md', 'input'),
        minHeight: getInputHeight(size || 'md'),
    }),
    {
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        position: 'relative',

        backgroundColor: colors.gray[100],
        padding: spacing.xxsm,
        transition: `${transition.duration.normal} all`,
        boxSizing: 'content-box',
    },
);

export const Required = styled.span({
    color: colors.red[500],
    marginLeft: spacing.xxsm,
});

export const StyledInput = styled.input<{
    customSize?: SizeOptions;
    disabled?: boolean;
    colorScheme: ColorOptions;
    checked?: boolean;
}>`
    opacity: 0;
    position: absolute;

    &:checked + ${Slider} {
        background-color: ${(props) => (!props.disabled ? colors[props.colorScheme][500] : colors.gray[100])};

        &:before {
            transform: ${({ customSize }) => getSliderTransformPosition(customSize || 'md')};
        }
    }

    &:focus-within + ${Slider} {
        border-color: ${(props) => (props.checked ? colors[props.colorScheme][200] : 'transparent')};
        outline: ${(props) => (props.checked ? `${borders['2px']} ${colors[props.colorScheme][200]}` : 'none')};
        box-shadow: ${(props) => (props.checked ? shadows.xs : 'none')};
    }
`;

export const StyledIcon = styled(Icon)<{ checked?: boolean; size: SizeOptions }>(
    ({ checked, size }) => ({
        left: getIconTransformPositionLeft(size, checked || false),
        top: getIconTransformPositionTop(size),
    }),
    {
        transition: `${transition.duration.normal} all`,
        position: 'absolute',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        color: colors.gray[500],
    },
);

export const IconContainer = styled.div({
    position: 'relative',
});
