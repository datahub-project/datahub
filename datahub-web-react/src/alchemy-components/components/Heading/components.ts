import styled from 'styled-components';

import { typography, colors } from '@components/theme';
import { getColor, getFontSize } from '@components/theme/utils';
import { HeadingProps } from './types';

const headingStyles = {
    H1: {
        fontSize: typography.fontSizes['4xl'],
        lineHeight: typography.lineHeights['2xl'],
    },
    H2: {
        fontSize: typography.fontSizes['3xl'],
        lineHeight: typography.lineHeights.xl,
    },
    H3: {
        fontSize: typography.fontSizes['2xl'],
        lineHeight: typography.lineHeights.lg,
    },
    H4: {
        fontSize: typography.fontSizes.xl,
        lineHeight: typography.lineHeights.lg,
    },
    H5: {
        fontSize: typography.fontSizes.lg,
        lineHeight: typography.lineHeights.md,
    },
    H6: {
        fontSize: typography.fontSizes.md,
        lineHeight: typography.lineHeights.xs,
    },
};

// Default styles
const baseStyles = {
    fontFamily: typography.fonts.heading,
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
    if (props.color) styles.color = getColor(props.color);
    if (props.weight) styles.fontWeight = typography.fontWeights[props.weight];
    if (isText) styles.lineHeight = typography.lineHeights[props.size];
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
