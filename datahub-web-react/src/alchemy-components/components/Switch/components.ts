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
        '&:before': {
            transition: `${transition.duration.normal} all`,
            content: '""',
            position: 'absolute',
            width: '18px',
            height: '18px',
            borderRadius: isSquare ? '0px' : '200px',
            top: '50%',
            left: '1px',
            transform: 'translate(0, -50%)',
            backgroundColor: !isDisabled ? colors.white : colors.gray[200],
            boxShadow: `
				0px 1px 2px 0px rgba(16, 24, 40, 0.06),
				0px 1px 3px 0px rgba(16, 24, 40, 0.12)
			`,
        },
        borderRadius: isSquare ? '0px' : '200px',
        width: '34px',
        height: '20px',
        minWidth: '34px',
        minHeight: '20px',
    }),
    {
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        position: 'relative',

        backgroundColor: colors.gray[100],
        padding: '0px',
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
        background: ${(props) => {
            if (props.disabled) return colors.gray[100];
            if (props.colorScheme === 'violet' || props.colorScheme === 'primary') {
                return 'linear-gradient(180deg, rgba(255, 255, 255, 0.20) 0%, rgba(83.44, 63, 209, 0.20) 100%), #533FD1';
            }
            return getColor(props.colorScheme, 500, props.theme);
        }};

        &:before {
            left: 15px;
            background: white;
            box-shadow: 0px 0px 2px rgba(14.17, 5.47, 67.64, 0.12);
        }
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
        left: checked ? '15px' : '1px',
        top: '50%',
        transform: 'translate(-50%, -50%)',
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
