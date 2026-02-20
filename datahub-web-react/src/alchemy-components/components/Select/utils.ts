import { SelectStyleProps } from '@components/components/Select/types';
import { radius, spacing, typography } from '@components/theme';
import { getFontSize } from '@components/theme/utils';

export const getOptionLabelStyle = (
    isSelected: boolean,
    isMultiSelect?: boolean,
    isDisabled?: boolean,
    applyHoverWidth?: boolean,
    themeColors?: { text: string; textSecondary: string; bgSurfaceBrand: string; bgHover: string },
) => {
    const color = isSelected ? themeColors?.text : themeColors?.textSecondary;
    const backgroundColor = !isDisabled && !isMultiSelect && isSelected ? themeColors?.bgSurfaceBrand : 'transparent';

    return {
        cursor: isDisabled ? 'not-allowed' : 'pointer',
        padding: spacing.xsm,
        borderRadius: radius.md,
        lineHeight: typography.lineHeights.normal,
        backgroundColor,
        color,
        fontWeight: typography.fontWeights.medium,
        fontSize: typography.fontSizes.md,
        display: 'flex',
        alignItems: 'center',
        width: applyHoverWidth ? '100%' : 'auto',
        '&:hover': {
            backgroundColor: isSelected ? themeColors?.bgSurfaceBrand : themeColors?.bgHover,
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

export const getSelectStyle = (
    props: SelectStyleProps,
    themeColors?: {
        border: string;
        bg: string;
        bgInputDisabled: string;
        text: string;
        textDisabled: string;
        textPlaceholder: string;
        borderBrandFocused: string;
        borderInputFocus: string;
        shadowXs: string;
        shadowSm: string;
    },
) => {
    const { isDisabled, isReadOnly, fontSize, isOpen } = props;

    const baseStyle = {
        borderRadius: radius.md,
        border: `1px solid ${themeColors?.border}`,
        fontFamily: typography.fonts.body,
        backgroundColor: isDisabled ? themeColors?.bgInputDisabled : themeColors?.bg,
        color: isDisabled ? themeColors?.textDisabled : themeColors?.text,
        cursor: isDisabled || isReadOnly ? 'not-allowed' : 'pointer',
        boxShadow: themeColors?.shadowXs,
        textWrap: 'nowrap',

        '&::placeholder': {
            color: themeColors?.textPlaceholder,
        },

        ...(isOpen
            ? {
                  borderColor: themeColors?.borderInputFocus,
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
