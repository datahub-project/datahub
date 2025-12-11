/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

import { Icon, IconNames } from '@components/components/Icon';
import { TextAreaProps } from '@components/components/TextArea/types';
import {
    formLabelTextStyles,
    inputPlaceholderTextStyles,
    inputValueTextStyles,
} from '@components/components/commonStyles';
import theme, { borders, colors, radius, sizes, spacing, typography } from '@components/theme';
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

export const TextAreaContainer = styled.div(
    ({ isSuccess, warning, isDisabled, isInvalid }: TextAreaProps) => ({
        border: `${borders['1px']} ${getStatusColors(isSuccess, warning, isInvalid)}`,
        backgroundColor: isDisabled ? colors.gray[1500] : colors.white,
    }),
    {
        ...defaultFlexStyles,
        position: 'relative',
        minWidth: '160px',
        minHeight,
        width: sizes.full,
        borderRadius: radius.md,
        flex: 1,
        color: colors.gray[400], // first icon color

        '&:focus-within': {
            borderColor: colors.violet[200],
            outline: `${borders['1px']} ${colors.violet[200]}`,
        },
    },
);

export const TextAreaField = styled.textarea<{ icon?: IconNames }>(({ icon }) => ({
    padding: `${spacing.sm} ${spacing.md}`,
    borderRadius: radius.md,
    border: borders.none,
    width: '100%',
    minHeight,

    ...inputValueTextStyles(),

    // Account for icon placement
    ...(icon && {
        paddingLeft: spacing.xsm,
    }),

    '&:focus': {
        outline: 'none',
    },

    '&::placeholder': {
        ...inputPlaceholderTextStyles,
    },

    '&:disabled': {
        backgroundColor: colors.gray[1500],
    },
}));

export const Label = styled.div({
    ...formLabelTextStyles,
    marginBottom: spacing.xxsm,
    textAlign: 'left',
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
    top: spacing.sm,
    right: spacing.sm,
});
