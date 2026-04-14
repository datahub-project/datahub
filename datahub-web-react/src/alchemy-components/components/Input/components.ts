import styled from 'styled-components';

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

export const InputContainer = styled.div<{
    isSuccess?: boolean;
    warning?: string;
    isDisabled?: boolean;
    isInvalid?: boolean;
}>(({ isSuccess, warning, isDisabled, isInvalid, theme }) => ({
    border: `${borders['1px']} ${getStatusColors(isSuccess, warning, isInvalid, theme.colors)}`,
    backgroundColor: isDisabled ? theme.colors.bgInputDisabled : theme.colors.bg,
    paddingRight: spacing.md,
    ...defaultFlexStyles,
    width: '100%',
    maxHeight: INPUT_MAX_HEIGHT,
    overflow: 'hidden' as const,
    borderRadius: radius.md,
    flex: 1,
    color: theme.colors.icon,
    boxShadow: theme.colors.shadowXs,

    '&:focus-within': {
        borderColor: theme.colors.borderBrandFocused,
        outline: `${borders['1px']} ${theme.colors.borderBrandFocused}`,
    },
}));

export const InputField = styled.input(({ theme }) => ({
    padding: `${spacing.sm} ${spacing.md}`,
    lineHeight: typography.lineHeights.normal,
    maxHeight: INPUT_MAX_HEIGHT,
    border: borders.none,
    width: '100%',
    backgroundColor: 'transparent',

    ...inputValueTextStyles(),
    color: theme.colors.text,

    '&::placeholder': {
        ...inputPlaceholderTextStyles,
        color: theme.colors.textPlaceholder,
    },

    '&:focus': {
        outline: 'none',
    },

    '&:disabled': {
        backgroundColor: theme.colors.bgInputDisabled,
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
