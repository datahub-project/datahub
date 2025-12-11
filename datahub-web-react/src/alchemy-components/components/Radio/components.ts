/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

import { getRadioBorderColor, getRadioCheckmarkColor } from '@components/components/Radio/utils';
import { formLabelTextStyles } from '@components/components/commonStyles';
import { borders, colors, radius, spacing } from '@components/theme';

export const RadioWrapper = styled.div<{ disabled: boolean; error: string }>(({ disabled, error, theme }) => ({
    position: 'relative',
    margin: '20px',
    width: '20px',
    height: '20px',
    border: `${borders['2px']} ${getRadioBorderColor(disabled, error)}`,
    backgroundColor: colors.white,
    borderRadius: radius.full,
    display: 'flex',
    justifyContent: 'flex-start',
    alignItems: 'center',
    marginRight: '40px',
    cursor: !disabled ? 'pointer' : 'none',
    transition: 'border 0.3s ease, outline 0.3s ease',
    '&:hover': {
        border: `${borders['2px']} ${!disabled && !error ? theme.styles['primary-color'] : getRadioBorderColor(disabled, error)}`,
        outline: !disabled && !error ? `${borders['2px']} ${colors.gray[200]}` : 'none',
    },
}));

export const RadioBase = styled.div({});

export const Label = styled.div({
    ...formLabelTextStyles,
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
});

export const RadioLabel = styled.div({
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
});

export const Required = styled.span({
    color: colors.red[500],
    marginLeft: spacing.xxsm,
});

export const RadioHoverState = styled.div({
    border: `${borders['2px']} ${(props) => props.theme.styles['primary-color']}`,
    width: 'calc(100% - -3px)',
    height: 'calc(100% - -3px)',
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    borderRadius: radius.full,
});

export const Checkmark = styled.div<{ checked: boolean; disabled: boolean; error: string }>(
    ({ checked, disabled, error }) => ({
        width: 'calc(100% - 6px)',
        height: 'calc(100% - 6px)',
        borderRadius: radius.full,
        background: getRadioCheckmarkColor(checked, disabled, error),
        display: checked ? 'inline-block' : 'none',
        position: 'absolute',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
    }),
);

export const HiddenInput = styled.input<{ checked: boolean }>({
    opacity: 0,
    width: '20px',
    height: '20px',
});

export const RadioGroupContainer = styled.div<{ isVertical?: boolean }>(({ isVertical }) => ({
    display: 'flex',
    flexDirection: isVertical ? 'column' : 'row',
    justifyContent: 'center',
    alignItems: 'center',
    gap: !isVertical ? spacing.md : spacing.none,
    margin: !isVertical ? spacing.xxsm : spacing.none,
}));
