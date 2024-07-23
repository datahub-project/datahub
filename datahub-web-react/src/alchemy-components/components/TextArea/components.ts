import styled from 'styled-components';

import theme, { colors, radius, borders, spacing, typography, sizes } from '@components/theme';
import { getStatusColors } from '@components/theme/utils';
import { Icon, IconNames } from '../Icon';
import { TextAreaProps } from './types';

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
});

export const TextAreaContainer = styled.div(
    ({ isSuccess, warning, isDisabled, isInvalid }: TextAreaProps) => ({
        border: `${borders['1px']} ${getStatusColors(isSuccess, warning, isInvalid)}`,
        backgroundColor: isDisabled ? colors.gray[100] : colors.white,
    }),
    {
        ...defaultFlexStyles,
        position: 'relative',
        minWidth: '160px',
        minHeight: '64px',
        width: sizes.full,
        borderRadius: radius.md,
        padding: spacing.xsm,
        flex: 1,
        color: colors.gray[400], // first icon color

        '&:focus-within': {
            borderColor: colors.violet[200],
            outline: `${borders['1px']} ${colors.violet[200]}`,
        },
    },
);

export const TextAreaField = styled.textarea<{ icon?: IconNames }>(({ icon }) => ({
    padding: spacing.none,
    border: borders.none,
    minWidth: icon ? '89% !important' : '-webkit-fill-available !important',
    minHeight: '64px',
    fontFamily: typography.fonts.body,

    '&:focus': {
        outline: 'none',
    },

    '&::placeholder': {
        color: colors.gray[400],
    },
}));

export const Label = styled.div({
    marginBottom: spacing.xxsm,
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes.md,
    textAlign: 'left',
    color: colors.gray[600],
});

export const Required = styled.span({
    color: colors.red[500],
});

export const ErrorMessage = styled.div({
    ...defaultMessageStyles,
    color: theme.semanticTokens.colors.error,
});

export const WarningMessage = styled.div({
    ...defaultMessageStyles,
    color: theme.semanticTokens.colors.warning,
});

export const StyledStatusIcon = styled(Icon)({
    position: 'absolute',
    top: spacing.xsm,
    right: spacing.xxsm,
});
