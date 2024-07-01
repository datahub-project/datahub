import styled from 'styled-components';

import { HeadingProps, TextProps } from './types';

import { text, colors, getColorValue, getFontSize } from '../theme';

const headingStyles = {
    H1: {
        fontSize: text.size['4xl'],
        lineHeight: text.lineHeight['2xl'],
    },
    H2: {
        fontSize: text.size['3xl'],
        lineHeight: text.lineHeight.xl,
    },
    H3: {
        fontSize: text.size['2xl'],
        lineHeight: text.lineHeight.lg,
    },
    H4: {
        fontSize: text.size.xl,
        lineHeight: text.lineHeight.lg,
    },
    H5: {
        fontSize: text.size.lg,
        lineHeight: text.lineHeight.md,
    },
    H6: {
        fontSize: text.size.md,
        lineHeight: text.lineHeight.xs,
    },
};

// Text Styles
const textStyles = {
    fontSize: text.size.md,
    lineHeight: text.lineHeight.md,
    fontWeight: text.weight.light,
};

// Default styles
const baseStyles = {
    fontFamily: text.family.default,
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
const propStyles = (props, isText = false) => {
    const styles = {} as any;
    if (props.size) styles.fontSize = getFontSize(props.size);
    if (props.color) styles.color = getColorValue(props.color);
    if (props.weight) styles.fontWeight = text.weight[props.weight];
    if (isText) styles.lineHeight = text.lineHeight[props.size];
    return styles;
};

// Generate Headings
const headings = {} as any;

['H1', 'H2', 'H3', 'H4', 'H5', 'H6'].forEach((heading) => {
    const component = styled[heading.toLowerCase()];
    headings[heading] = component({ ...baseStyles, ...headingStyles[heading] }, (props: HeadingProps) => ({
        ...propStyles(props as HeadingProps),
    }));
});

export const { H1, H2, H3, H4, H5, H6 } = headings;

export const P = styled.p({ ...baseStyles, ...textStyles }, (props: TextProps) => ({
    ...propStyles(props as TextProps, true),
}));

export const Span = styled.span({ ...baseStyles, ...textStyles }, (props: TextProps) => ({
    ...propStyles(props as TextProps, true),
}));

export const Div = styled.div({ ...baseStyles, ...textStyles }, (props: TextProps) => ({
    ...propStyles(props as TextProps, true),
}));
