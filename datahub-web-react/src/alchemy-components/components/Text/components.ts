/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import styled from 'styled-components';

import { TextProps } from '@components/components/Text/types';
import { colors, typography } from '@components/theme';
import { getColor, getFontSize } from '@components/theme/utils';

// Text Styles
const textStyles = {
    fontSize: typography.fontSizes.md,
    lineHeight: typography.lineHeights.md,
    fontWeight: typography.fontWeights.normal,
};

// Default styles
const baseStyles = {
    fontFamily: typography.fonts.body,
    margin: 0,

    '& a': {
        color: colors.violet[400],
        textDecoration: 'none',
        transition: 'color 0.15s ease',

        '&:hover': {
            color: colors.violet[500],
        },
    },
};

// Prop Driven Styles
const propStyles = (props: TextProps, isText = false) => {
    const styles = {} as any;
    if (props.size) styles.fontSize = getFontSize(props.size);
    if (props.color) styles.color = getColor(props.color, props.colorLevel, props.theme);
    if (props.weight) styles.fontWeight = typography.fontWeights[props.weight];
    if (isText) styles.lineHeight = typography.lineHeights[props.lineHeight || props.size || 'md'];
    return styles;
};

export const P = styled.p({ ...baseStyles, ...textStyles }, (props: TextProps) => ({
    ...propStyles(props as TextProps, true),
}));

export const Span = styled.span({ ...baseStyles, ...textStyles }, (props: TextProps) => ({
    ...propStyles(props as TextProps, true),
}));

export const Div = styled.div({ ...baseStyles, ...textStyles }, (props: TextProps) => ({
    ...propStyles(props as TextProps, true),
}));

export const Pre = styled.pre({ ...baseStyles, ...textStyles }, (props: TextProps) => ({
    ...propStyles(props as TextProps, true),
}));
