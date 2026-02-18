import styled from 'styled-components';

import { Icon } from '@components/components/Icon';
import type { SwitchLabelPosition } from '@components/components/Switch/types';
import {
    getIconTransformPositionLeft,
    getIconTransformPositionTop,
    getInputHeight,
    getSliderTransformPosition,
    getToggleSize,
} from '@components/components/Switch/utils';
import { formLabelTextStyles } from '@components/components/commonStyles';
import { borders, shadows, spacing, transition } from '@components/theme';
import { ColorOptions, SizeOptions } from '@components/theme/config';
import { getColor } from '@components/theme/utils';

export const Label = styled.div(({ theme }) => ({
    ...formLabelTextStyles,
    color: theme.colors.text,
    display: 'flex',
    alignItems: 'flex-start',
}));

export const SwitchContainer = styled.label<{
    labelPosition: SwitchLabelPosition;
    isDisabled?: boolean;
}>(({ labelPosition, isDisabled }) => {
    const styles: any = {
        display: 'flex',
        alignItems: 'center',
        gap: spacing.sm,
        cursor: isDisabled ? 'not-allowed' : 'pointer',
        width: 'max-content',
    };

    if (labelPosition === 'top') {
        styles.flexDirection = 'column';
        styles.alignItems = 'flex-start';
    } else if (labelPosition === 'right') {
        styles.flexDirection = 'row-reverse';
    } else {
        styles.flexDirection = 'row';
    }

    return styles;
});

export const Slider = styled.div<{ size?: SizeOptions; isSquare?: boolean; isDisabled?: boolean }>(
    ({ size, isSquare, isDisabled, theme }) => ({
        '&:before': {
            transition: `${transition.duration.normal} all`,
            content: '""',
            position: 'absolute',
            minWidth: getToggleSize(size || 'md', 'slider'),
            minHeight: getToggleSize(size || 'md', 'slider'),
            borderRadius: !isSquare ? '35px' : '0px',
            top: '50%',
            left: '2px',
            transform: 'translate(0, -50%)',
            backgroundColor: !isDisabled ? theme.colors.bg : theme.colors.border,
            boxShadow: theme.colors.shadowXs,
        },
        borderRadius: !isSquare ? '32px' : '0px',
        minWidth: getToggleSize(size || 'md', 'input'),
        minHeight: getInputHeight(size || 'md'),
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        position: 'relative',
        backgroundColor: theme.colors.border,
        padding: '2px',
        transition: `${transition.duration.normal} all`,
        boxSizing: 'content-box',
    }),
);

export const Required = styled.span(({ theme }) => ({
    color: theme.colors.textError,
    marginLeft: spacing.xxsm,
}));

export const StyledInput = styled.input<{
    customSize?: SizeOptions;
    disabled?: boolean;
    colorScheme: ColorOptions;
    checked?: boolean;
}>`
    opacity: 0;
    position: absolute;

    &:checked + ${Slider} {
        background: ${(props) => (!props.disabled ? props.theme.colors.brandGradient : props.theme.colors.bgDisabled)};

        &:before {
            transform: ${({ customSize }) => getSliderTransformPosition(customSize || 'md')};
        }
    }

    &:focus-within + ${Slider} {
        border-color: ${(props) => (props.checked ? getColor(props.colorScheme, 200, props.theme) : 'transparent')};
        outline: ${(props) =>
            props.checked ? `${borders['2px']} ${getColor(props.colorScheme, 200, props.theme)}` : 'none'};
        box-shadow: ${(props) => (props.checked ? shadows.xs : 'none')};
    }
`;

export const StyledIcon = styled(Icon)<{ checked?: boolean; size: SizeOptions }>(({ checked, size, theme }) => ({
    left: getIconTransformPositionLeft(size, checked || false),
    top: getIconTransformPositionTop(size),
    color: checked ? theme.colors.iconBrand : theme.colors.icon,
    transition: `${transition.duration.normal} all`,
    position: 'absolute',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
}));

export const IconContainer = styled.div({
    position: 'relative',
});
