import styled from 'styled-components';

import { TextProps } from '@components/components/Text/types';
import { typography } from '@components/theme';
import { ColorOptions } from '@components/theme/config';
import { getColor, getFontSize } from '@components/theme/utils';

import { Theme } from '@conf/theme/types';

type ThemedTextProps = TextProps & { theme: Theme };

const textStyles = {
    fontSize: typography.fontSizes.md,
    lineHeight: typography.lineHeights.md,
    fontWeight: typography.fontWeights.normal,
};

const baseStyles = {
    fontFamily: typography.fonts.body,
    margin: 0,
};

// Prop Driven Styles
const propStyles = (props: ThemedTextProps, isText = false) => {
    const styles = {} as any;
    if (props.size) styles.fontSize = getFontSize(props.size);
    if (props.color) {
        const semantic = props.theme.colors[props.color];
        styles.color =
            typeof semantic === 'string'
                ? semantic
                : getColor(props.color as ColorOptions, props.colorLevel, props.theme);
    }
    if (props.weight) styles.fontWeight = typography.fontWeights[props.weight];
    if (isText) styles.lineHeight = typography.lineHeights[props.lineHeight || props.size || 'md'];
    return styles;
};

const themeAwareOverrides = (props: ThemedTextProps) => ({
    '& a': {
        color: props.theme.colors.hyperlinks,
        textDecoration: 'none',
        transition: 'color 0.15s ease',
        '&:hover': {
            color: props.theme.colors.textBrand,
        },
    },
});

export const P = styled.p({ ...baseStyles, ...textStyles }, (props: ThemedTextProps) => ({
    ...propStyles(props, true),
    ...themeAwareOverrides(props),
}));

export const Span = styled.span({ ...baseStyles, ...textStyles }, (props: ThemedTextProps) => ({
    ...propStyles(props, true),
    ...themeAwareOverrides(props),
}));

export const Div = styled.div({ ...baseStyles, ...textStyles }, (props: ThemedTextProps) => ({
    ...propStyles(props, true),
    ...themeAwareOverrides(props),
}));

export const Pre = styled.pre({ ...baseStyles, ...textStyles }, (props: ThemedTextProps) => ({
    ...propStyles(props, true),
    ...themeAwareOverrides(props),
}));
