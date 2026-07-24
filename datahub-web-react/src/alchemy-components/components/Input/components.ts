import styled, { css } from 'styled-components';

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

export const inputContainerStyles = css<{
    isSuccess?: boolean;
    warning?: string;
    isDisabled?: boolean;
    isInvalid?: boolean;
}>`
    display: flex;
    justify-content: space-between;
    align-items: center;
    width: 100%;
    flex: 1;
    max-height: ${INPUT_MAX_HEIGHT};
    padding-right: ${spacing.md};
    overflow: hidden;
    border-radius: ${radius.md};
    border: ${({ isSuccess, warning, isInvalid, theme }) =>
        `${borders['1px']} ${getStatusColors(isSuccess, warning, isInvalid, theme.colors)}`};
    background-color: ${({ isDisabled, theme }) => (isDisabled ? theme.colors.bgInputDisabled : theme.colors.bg)};
    color: ${({ theme }) => theme.colors.icon};
    box-shadow: ${({ theme }) => theme.colors.shadowXs};

    &:focus-within {
        border-color: ${({ theme }) => theme.colors.borderBrandFocused};
        outline: ${({ theme }) => `${borders['1px']} ${theme.colors.borderBrandFocused}`};
    }
`;

export const InputContainer = styled.div<{
    isSuccess?: boolean;
    warning?: string;
    isDisabled?: boolean;
    isInvalid?: boolean;
}>`
    ${inputContainerStyles}
`;

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

export const Label = styled.label(({ theme }) => ({
    ...formLabelTextStyles,
    color: theme.colors.text,
    marginBottom: spacing.xxsm,
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
