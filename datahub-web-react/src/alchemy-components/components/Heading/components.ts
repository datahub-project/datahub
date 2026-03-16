import styled from 'styled-components';

import { HeadingStyleProps } from '@components/components/Heading/types';
import { colors, typography } from '@components/theme';
import { getColor, getFontSize } from '@components/theme/utils';

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

const semanticHeadingColors: Record<string, 'text'> = {
    H1: 'text',
    H2: 'text',
    H3: 'text',
    H4: 'text',
    H5: 'text',
    H6: 'text',
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
const propStyles = (props: HeadingStyleProps & { theme?: any }, isText = false, headingLevel?: string) => {
    const styles = {} as any;
    if (props.size) styles.fontSize = getFontSize(props.size);
    if (props.color && props.color !== 'inherit') {
        styles.color = getColor(props.color, props.colorLevel);
    } else if (headingLevel && props.theme?.colors) {
        const tokenKey = semanticHeadingColors[headingLevel];
        styles.color = props.theme.colors[tokenKey];
    }
    if (props.weight) styles.fontWeight = typography.fontWeights[props.weight];
    if (isText) styles.lineHeight = typography.lineHeights[props.size];
    return styles;
};

// Generate Headings
const headings = {} as any;

['H1', 'H2', 'H3', 'H4', 'H5', 'H6'].forEach((heading) => {
    const component = styled[heading.toLowerCase()];
    headings[heading] = component(
        { ...baseStyles, ...headingStyles[heading] },
        (props: HeadingStyleProps & { theme?: any }) => ({
            ...propStyles(props, false, heading),
        }),
    );
});

export const { H1, H2, H3, H4, H5, H6 } = headings;
