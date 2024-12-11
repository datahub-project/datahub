import { borders, colors, radius, spacing, typography } from '@components/theme';
import { getFontSize } from '@components/theme/utils';

import { SelectStyleProps } from './types';

export const getOptionLabelStyle = (isSelected: boolean, isMultiSelect?: boolean, isDisabled?: boolean) => ({
    cursor: isDisabled ? 'not-allowed' : 'pointer',
    padding: spacing.xsm,
    borderRadius: radius.md,
    lineHeight: typography.lineHeights.normal,
    backgroundColor: isSelected && !isMultiSelect ? colors.violet[100] : 'transparent',
    color: isSelected ? colors.violet[700] : colors.gray[500],
    fontWeight: typography.fontWeights.medium,
    fontSize: typography.fontSizes.md,
    display: 'flex',
    alignItems: 'center',

    '&:hover': {
        backgroundColor: isSelected ? colors.violet[100] : colors.gray[100],
    },
});

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
            padding: `${spacing.sm} ${spacing.xsm}`,
        },
        md: {
            padding: `${spacing.sm} ${spacing.md}`,
        },
        lg: {
            padding: `${spacing.md} ${spacing.sm}`,
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

export const getSelectStyle = (props: SelectStyleProps) => {
    const { isDisabled, isReadOnly, fontSize, isOpen } = props;

    const baseStyle = {
        borderRadius: radius.md,
        border: `1px solid ${colors.gray[200]}`,
        fontFamily: typography.fonts.body,
        color: isDisabled ? colors.gray[300] : colors.black,
        cursor: isDisabled || isReadOnly ? 'not-allowed' : 'pointer',
        backgroundColor: isDisabled ? colors.gray[100] : 'initial',

        '&::placeholder': {
            color: colors.gray[400],
        },

        // Open Styles
        ...(isOpen
            ? {
                  borderColor: colors.violet[300],
                  boxShadow: `0px 0px 4px 0px rgba(83, 63, 209, 0.5)`,
                  outline: 'none',
              }
            : {}),

        // Hover Styles
        ...(isDisabled || isReadOnly || isOpen
            ? {}
            : {
                  '&:hover': {
                      borderColor: colors.violet[200],
                      outline: `${borders['1px']} ${colors.violet[200]}`,
                  },
              }),
    };

    const fontStyles = getSelectFontStyles(fontSize);
    const paddingStyles = getSelectPadding(fontSize);

    return {
        ...baseStyle,
        ...fontStyles,
        ...paddingStyles,
    };
};
