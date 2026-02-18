import styled from 'styled-components';

import { Icon, IconNames } from '@components/components/Icon';
import {
    formLabelTextStyles,
    inputPlaceholderTextStyles,
    inputValueTextStyles,
} from '@components/components/commonStyles';
import { borders, radius, sizes, spacing, typography } from '@components/theme';
import { getStatusColors } from '@components/theme/utils';

const minHeight = '100px';

const defaultFlexStyles = {
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
};

const defaultMessageStyles = {
    marginTop: spacing.xxsm,
    fontSize: typography.fontSizes.sm,
};

export const TextAreaWrapper = styled.div({
    ...defaultFlexStyles,
    flexDirection: 'column',
    width: '100%',
});

export const StyledIcon = styled(Icon)({
    minWidth: '16px',
    paddingLeft: spacing.sm,
    marginTop: spacing.sm,
});

export const TextAreaContainer = styled.div<{
    isSuccess?: boolean;
    warning?: string;
    isDisabled?: boolean;
    isInvalid?: boolean;
}>(({ isSuccess, warning, isDisabled, isInvalid, theme }) => ({
    border: `${borders['1px']} ${getStatusColors(isSuccess, warning, isInvalid, theme.colors)}`,
    backgroundColor: isDisabled ? theme.colors.bgSurface : theme.colors.bg,
    ...defaultFlexStyles,
    position: 'relative' as const,
    minWidth: '160px',
    minHeight,
    width: sizes.full,
    borderRadius: radius.md,
    flex: 1,
    color: theme.colors.icon,
    boxShadow: theme.colors.shadowXs,

    '&:focus-within': {
        borderColor: theme.colors.borderBrandFocused,
        outline: `${borders['1px']} ${theme.colors.borderBrandFocused}`,
    },
}));

export const TextAreaField = styled.textarea<{ icon?: IconNames }>(({ icon, theme }) => ({
    padding: `${spacing.sm} ${spacing.md}`,
    borderRadius: radius.md,
    border: borders.none,
    width: '100%',
    minHeight,
    backgroundColor: 'transparent',

    ...inputValueTextStyles(),
    color: theme.colors.text,

    ...(icon && {
        paddingLeft: spacing.xsm,
    }),

    '&:focus': {
        outline: 'none',
    },

    '&::placeholder': {
        ...inputPlaceholderTextStyles,
        color: theme.colors.textTertiary,
    },

    '&:disabled': {
        backgroundColor: theme.colors.bgSurface,
    },
}));

export const Label = styled.div(({ theme }) => ({
    ...formLabelTextStyles,
    color: theme.colors.text,
    marginBottom: spacing.xxsm,
    textAlign: 'left',
}));

export const Required = styled.span(({ theme }) => ({
    color: theme.colors.textError,
}));

export const ErrorMessage = styled.div(({ theme: t }) => ({
    ...defaultMessageStyles,
    color: t.colors.textError,
}));

export const WarningMessage = styled.div(({ theme: t }) => ({
    ...defaultMessageStyles,
    color: t.colors.textWarning,
}));

export const StyledStatusIcon = styled(Icon)({
    position: 'absolute',
    top: spacing.sm,
    right: spacing.sm,
});
