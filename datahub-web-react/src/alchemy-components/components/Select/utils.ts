import { colors, radius, spacing, typography } from '@components/theme';
import { getFontSize } from '@components/theme/utils';

import { SelectStyleProps } from './types';

export const getOptionLabelStyle = (isSelected: boolean, isMultiSelect?: boolean, isDisabled?: boolean) => {
    const color = isSelected ? colors.gray[600] : colors.gray[500];
    const backgroundColor = !isDisabled && !isMultiSelect && isSelected ? colors.gray[1000] : 'transparent';

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

        '&:hover': {
            backgroundColor: isSelected ? colors.violet[100] : colors.gray[100],
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
            padding: `${spacing.xxsm} ${spacing.xxsm}`,
        },
        md: {
            padding: `${spacing.xsm} ${spacing.xsm}`,
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

export const getSelectStyle = (props: SelectStyleProps) => {
    const { isDisabled, isReadOnly, fontSize, isOpen } = props;

    const baseStyle = {
        borderRadius: radius.md,
        border: `1px solid ${isDisabled ? colors.gray[1800] : colors.gray[100]}`,
        fontFamily: typography.fonts.body,
        color: isDisabled ? colors.gray[300] : colors.gray[600],
        cursor: isDisabled || isReadOnly ? 'not-allowed' : 'pointer',
        backgroundColor: isDisabled ? colors.gray[1500] : 'initial',
        boxShadow: '0px 1px 2px 0px rgba(33, 23, 95, 0.07)',
        textWrap: 'nowrap',

        '&::placeholder': {
            color: colors.gray[1900],
        },

        // Open Styles
        ...(isOpen
            ? {
                  borderColor: colors.gray[1800],
                  outline: `2px solid ${colors.violet[300]}`,
              }
            : {}),

        // Hover Styles
        ...(isDisabled || isReadOnly || isOpen
            ? {}
            : {
                  '&:hover': {
                      boxShadow: '0px 1px 2px 1px rgba(33, 23, 95, 0.07)',
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
