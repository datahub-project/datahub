import styled from 'styled-components';

import { HeadingStyleProps } from '@components/components/Heading/types';
import { typography } from '@components/theme';
import { ColorOptions } from '@components/theme/config';
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

const baseStyles = {
    fontFamily: typography.fonts.heading,
    margin: 0,
};

// Prop Driven Styles
const propStyles = (props: HeadingStyleProps & { theme?: any }, isText = false, headingLevel?: string) => {
    const styles = {} as any;
    if (props.size) styles.fontSize = getFontSize(props.size);
    if (props.color) {
        const semantic = props.theme.colors[props.color];
        styles.color =
            typeof semantic === 'string' ? semantic : getColor(props.color as ColorOptions, props.colorLevel);
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
    headings[heading] = component({ ...baseStyles, ...headingStyles[heading] }, (props: HeadingStyleProps) => ({
        ...propStyles(props),
        '& a': {
            color: props.theme.colors.hyperlinks,
            textDecoration: 'none',
            transition: 'color 0.15s ease',
            '&:hover': {
                color: props.theme.colors.textBrand,
            },
        },
    }));
});

export const { H1, H2, H3, H4, H5, H6 } = headings;
