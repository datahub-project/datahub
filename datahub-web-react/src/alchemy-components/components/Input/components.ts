import styled from 'styled-components';

import theme, { borders, colors, radius, spacing, typography } from '@components/theme';

import { getStatusColors } from '@components/theme/utils';

import type { InputProps } from './types';

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
    ({ isSuccess, warning, isDisabled, isInvalid }: InputProps) => ({
        border: `${borders['1px']} ${getStatusColors(isSuccess, warning, isInvalid)}`,
        backgroundColor: isDisabled ? colors.gray[100] : colors.white,
    }),
    {
        ...defaultFlexStyles,
        width: '100%',
        padding: `${spacing.sm} ${spacing.xsm}`,
        borderRadius: radius.md,
        flex: 1,
        color: colors.gray[400], // 1st icon color

        '&:focus-within': {
            borderColor: colors.violet[200],
            outline: `${borders['1px']} ${colors.violet[200]}`,
        },
    },
);

export const InputField = styled.input({
    padding: spacing.none,
    border: borders.none,
    margin: `0 ${spacing.xsm}`,
    width: '100%',

    '&::placeholder': {
        color: colors.gray[400],
    },

    '&:focus': {
        outline: 'none',
    },
});

export const Required = styled.span({
    color: colors.red[500],
});

export const Label = styled.div({
    marginBottom: spacing.xxsm,
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes.md,
    textAlign: 'left',
    color: colors.gray[600],
});

export const ErrorMessage = styled.div({
    ...defaultMessageStyles,
    color: theme.semanticTokens.colors.error,
});

export const WarningMessage = styled.div({
    ...defaultMessageStyles,
    color: theme.semanticTokens.colors.warning,
});
