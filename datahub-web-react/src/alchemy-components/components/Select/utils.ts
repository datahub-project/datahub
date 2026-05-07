import { DefaultTheme } from 'styled-components';

import { SelectStyleProps } from '@components/components/Select/types';
import { radius, spacing, typography } from '@components/theme';
import { getFontSize } from '@components/theme/utils';

export const getOptionLabelStyle = (
    isSelected: boolean,
    isMultiSelect?: boolean,
    isDisabled?: boolean,
    applyHoverWidth?: boolean,
    theme?: DefaultTheme,
) => {
    const themeColors = theme?.colors;

    const getColor = () => {
        if (isDisabled) return themeColors?.textDisabled;
        if (isSelected) return themeColors?.text;
        return themeColors?.textSecondary;
    };

    return {
        cursor: isDisabled ? 'not-allowed' : 'pointer',
        padding: spacing.xsm,
        borderRadius: radius.md,
        lineHeight: typography.lineHeights.normal,
        backgroundColor:
            !isDisabled && !isMultiSelect && isSelected ? (themeColors?.bgSelected ?? 'transparent') : 'transparent',
        color: getColor(),
        fontWeight: typography.fontWeights.medium,
        fontSize: typography.fontSizes.md,
        display: 'flex',
        alignItems: 'center',
        width: applyHoverWidth ? '100%' : 'auto',
        '&:hover': {
            backgroundColor: isSelected ? themeColors?.bgSelected : themeColors?.bgHover,
        },
    };
};

export const getFooterButtonSize = (size) => {
    return size === 'sm' ? 'sm' : 'md';
};

export const getSelectFontStyles = (size) => {
    const baseFontStyles = {
        lineHeight: typography.lineHeights.none,
    };

    const sizeStyles = {
        sm: {
            ...baseFontStyles,
            fontSize: getFontSize(size),
        },
        md: {
            ...baseFontStyles,
            fontSize: getFontSize(size),
        },
        lg: {
            ...baseFontStyles,
            fontSize: getFontSize(size),
        },
    };

    return sizeStyles[size];
};

export const getSelectPadding = (size) => {
    const paddingStyles = {
        sm: {
            padding: `${spacing.xxsm} ${spacing.xsm}`,
        },
        md: {
            padding: `${spacing.xxsm} ${spacing.xsm}`,
        },
        lg: {
            padding: `${spacing.sm} ${spacing.sm}`,
        },
    };

    return paddingStyles[size];
};

export const getSearchPadding = (size) => {
    const paddingStyles = {
        sm: {
            padding: `${spacing.xxsm} ${spacing.xsm}`,
        },
        md: {
            padding: `${spacing.xsm} ${spacing.xsm}`,
        },
        lg: {
            padding: `${spacing.xsm} ${spacing.xsm}`,
        },
    };

    return paddingStyles[size];
};

export const getMinHeight = (size) => {
    const minHeightStyles = {
        sm: {
            minHeight: '32px',
        },
        md: {
            minHeight: '42px',
        },
        lg: {
            minHeight: '42px',
        },
    };

    return minHeightStyles[size];
};

export const getSelectStyle = (props: SelectStyleProps & { theme?: DefaultTheme }) => {
    const { isDisabled, isReadOnly, fontSize, isOpen, theme } = props;
    const themeColors = theme?.colors;

    const baseStyle = {
        borderRadius: radius.md,
        border: `1px solid ${themeColors?.border}`,
        fontFamily: typography.fonts.body,
        backgroundColor: isDisabled ? themeColors?.bgInputDisabled : themeColors?.bgInput,
        color: isDisabled ? themeColors?.textDisabled : themeColors?.textSecondary,
        cursor: isDisabled || isReadOnly ? 'not-allowed' : 'pointer',
        boxShadow: themeColors?.shadowXs,
        textWrap: 'nowrap',

        '&::placeholder': {
            color: themeColors?.textTertiary,
        },

        ...(isOpen
            ? {
                  borderColor: themeColors?.borderBrand,
                  outline: `1px solid ${themeColors?.borderBrandFocused}`,
              }
            : {}),

        ...(isDisabled || isReadOnly || isOpen
            ? {}
            : {
                  '&:hover': {
                      boxShadow: themeColors?.shadowSm,
                  },
              }),
    };

    const fontStyles = getSelectFontStyles(fontSize);
    const paddingStyles = getSelectPadding(fontSize);
    const minHeightStyles = getMinHeight(fontSize);

    return {
        ...baseStyle,
        ...fontStyles,
        ...paddingStyles,
        ...minHeightStyles,
    };
};

export const getDropdownStyle = () => {
    const baseStyle = {
        fontFamily: typography.fonts.body,
    };

    return { ...baseStyle };
};
