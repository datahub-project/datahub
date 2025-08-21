import styled from 'styled-components';

import { Icon } from '@components/components/Icon';
import type { SwitchLabelPosition } from '@components/components/Switch/types';
import { formLabelTextStyles } from '@components/components/commonStyles';
import { borders, colors, shadows, spacing, transition } from '@components/theme';
import { ColorOptions, SizeOptions } from '@components/theme/config';
import { getColor } from '@components/theme/utils';

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
    ({ isSquare, isDisabled }) => ({
        borderRadius: isSquare ? '0px' : '200px',
        width: '34px',
        height: '20px',
        minWidth: '34px',
        minHeight: '20px',
        position: 'relative',
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        backgroundColor: isDisabled ? colors.gray[100] : colors.gray[100],
        padding: '0px',
        transition: `${transition.duration.normal} all`,
        boxSizing: 'content-box',
    }),
);

export const ToggleButton = styled.div<{ checked?: boolean; isDisabled?: boolean }>`
    position: absolute;
    width: 18px;
    height: 18px;
    border-radius: 200px;
    background-color: ${({ isDisabled }) => (!isDisabled ? colors.white : colors.gray[1500])};
    box-shadow:
        0px 1px 2px 0px rgba(16, 24, 40, 0.06),
        0px 1px 3px 0px rgba(16, 24, 40, 0.12);
    transition: ${transition.duration.normal} all;
    left: ${({ checked }) => (checked ? '15px' : '1px')};
    top: 50%;
    transform: translateY(-50%);
    display: flex;
    align-items: center;
    justify-content: center;
`;

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
        background: ${(props) => {
            if (props.disabled) return colors.gray[100];
            if (props.colorScheme === 'violet' || props.colorScheme === 'primary') {
                return 'linear-gradient(180deg, rgba(255, 255, 255, 0.20) 0%, rgba(83.44, 63, 209, 0.20) 100%), #533FD1';
            }
            return getColor(props.colorScheme, 500, props.theme);
        }};
    }

    &:focus-within + ${Slider} {
        border-color: ${(props) => (props.checked ? getColor(props.colorScheme, 200, props.theme) : 'transparent')};
        outline: ${(props) =>
            props.checked ? `${borders['2px']} ${getColor(props.colorScheme, 200, props.theme)}` : 'none'};
        box-shadow: ${(props) => (props.checked ? shadows.xs : 'none')};
    }
`;

export const StyledIcon = styled(Icon)<{ checked?: boolean; size: SizeOptions }>(
    ({ checked }) => ({
        color: checked ? colors.violet[500] : colors.gray[500],
        width: '14px',
        height: '14px',
    }),
    {
        transition: `${transition.duration.normal} all`,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
    },
);
