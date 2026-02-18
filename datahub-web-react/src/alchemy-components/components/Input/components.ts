import styled from 'styled-components';

import type { InputProps } from '@components/components/Input/types';
import {
    INPUT_MAX_HEIGHT,
    formLabelTextStyles,
    inputPlaceholderTextStyles,
    inputValueTextStyles,
} from '@components/components/commonStyles';
import { borders, radius, spacing, typography } from '@components/theme';
import { getStatusColors } from '@components/theme/utils';

const defaultFlexStyles = {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
};

const defaultMessageStyles = {
    marginTop: spacing.xxsm,
    fontSize: typography.fontSizes.sm,
};

export const InputWrapper = styled.div({
    ...defaultFlexStyles,
    alignItems: 'flex-start',
    flexDirection: 'column',
    width: '100%',
});

export const InputContainer = styled.div(
    ({ isSuccess, warning, isDisabled, isInvalid, theme }: InputProps & { theme: any }) => ({
        border: `${borders['1px']} ${getStatusColors(isSuccess, warning, isInvalid)}`,
        backgroundColor: isDisabled ? theme.colors.bgSurface : theme.colors.bg,
        paddingRight: spacing.md,
        ...defaultFlexStyles,
        width: '100%',
        maxHeight: INPUT_MAX_HEIGHT,
        overflow: 'hidden',
        borderRadius: radius.md,
        flex: 1,
        color: theme.colors.icon,

        '&:focus-within': {
            borderColor: theme.colors.borderBrandFocused,
            outline: `${borders['1px']} ${theme.colors.borderBrandFocused}`,
        },
    }),
);

export const InputField = styled.input(({ theme }) => ({
    padding: `${spacing.sm} ${spacing.md}`,
    lineHeight: typography.lineHeights.normal,
    maxHeight: INPUT_MAX_HEIGHT,
    border: borders.none,
    width: '100%',
    backgroundColor: 'transparent',
    color: theme.colors.text,

    ...inputValueTextStyles(),

    '&::placeholder': {
        ...inputPlaceholderTextStyles,
        color: theme.colors.textTertiary,
    },

    '&:focus': {
        outline: 'none',
    },

    '&:disabled': {
        backgroundColor: theme.colors.bgSurface,
        cursor: 'not-allowed',
    },
}));

export const Required = styled.span(({ theme }) => ({
    color: theme.colors.textError,
}));

export const Label = styled.div(({ theme }) => ({
    ...formLabelTextStyles,
    color: theme.colors.text,
    marginBottom: spacing.xsm,
    textAlign: 'left',
}));

export const ErrorMessage = styled.div(({ theme: t }) => ({
    ...defaultMessageStyles,
    color: t.colors.textError,
}));

export const WarningMessage = styled.div(({ theme: t }) => ({
    ...defaultMessageStyles,
    color: t.colors.textWarning,
}));

export const HelperText = styled.div(({ theme }) => ({
    ...defaultMessageStyles,
    color: theme.colors.textSecondary,
}));
