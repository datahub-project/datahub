import { ColorOptions, SizeOptions } from '@components/theme/config';
import { borders, colors, shadows, spacing, transition, typography } from '@components/theme';
import styled from 'styled-components';
import { Icon } from '../Icon';
import {
    getIconTransformPositionLeft,
    getIconTransformPositionTop,
    getInputHeight,
    getSliderTransformPosition,
    getToggleSize,
} from './utils';

export const Label = styled.div({
    display: 'flex',
    alignItems: 'flex-start',
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes.md,
});

export const SwitchContainer = styled.label({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.sm,
    cursor: 'pointer',
});

export const Slider = styled.div<{ size?: SizeOptions; isSquare?: boolean; isDisabled?: boolean }>(
    ({ size, isSquare, isDisabled }) => ({
        '&:before': {
            transition: `${transition.duration.slow} all`,
            content: '""',
            position: 'absolute',
            minWidth: getToggleSize(size || 'md', 'slider'), // sliders width and height must be same
            minHeight: getToggleSize(size || 'md', 'slider'),
            borderRadius: !isSquare ? '35px' : '0px',
            top: '50%',
            left: spacing.xxsm,
            transform: 'translate(0, -50%)',
            backgroundColor: !isDisabled ? colors.white : colors.gray[200],
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
        padding: `${parseInt(spacing.xxsm, 10) + 2}px`,
        transition: `${transition.duration.slow} all`,
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
        outline: ${(props) => (props.checked ? `${borders['4px']} ${colors[props.colorScheme][200]}` : 'none')};
        box-shadow: ${(props) => (props.checked ? shadows.xs : 'none')};
    }
`;

export const StyledIcon = styled(Icon)<{ checked?: boolean; size: SizeOptions }>(
    ({ checked, size }) => ({
        left: getIconTransformPositionLeft(size, checked || false),
        top: getIconTransformPositionTop(size),
    }),
    {
        transition: `${transition.duration.slow} all`,
        position: 'absolute',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
    },
);

export const IconContainer = styled.div({
    position: 'relative',
});
