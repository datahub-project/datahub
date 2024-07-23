import { colors, radius, typography, spacing } from '@components/theme';
import { getFontSize } from '@components/theme/utils';
import { SelectStyleProps } from './types';

export const getOptionLabelStyle = (isSelected: boolean) => ({
    cursor: 'pointer',
    padding: spacing.xsm,
    borderRadius: radius.md,
    lineHeight: '20px',
    backgroundColor: isSelected ? colors.violet[50] : 'transparent',
    '&:hover': {
        backgroundColor: isSelected ? colors.violet[50] : colors.violet[25],
    },
    color: colors.gray[400],
    fontWeight: 500,
});

export const getFooterButtonSize = (size) => {
    return size === 'sm' ? 'sm' : 'md';
};

export const getSelectFontStyles = (size) => {
    const baseFontStyles = {
        fontFamily: typography.fonts.body,
        fontWeight: typography.fontWeights.light,
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
            padding: `${spacing.xsm} ${spacing.sm}`,
        },
        md: {
            padding: `${spacing.sm} ${spacing.sm}`,
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
    const { isDisabled, isReadOnly, fontSize } = props;

    const baseStyle = {
        pdding: spacing.xsm,
        borderRadius: radius.md,
        border: `1px solid ${colors.gray[200]}`,
        fontFamily: typography.fonts.body,
        color: isDisabled ? colors.gray[300] : colors.black,
        cursor: isDisabled || isReadOnly ? 'not-allowed' : 'pointer',
        backgroundColor: isDisabled ? colors.gray[100] : 'initial',
        ...(isDisabled || isReadOnly
            ? {}
            : {
                  '&:hover': {
                      borderColor: colors.violet[600],
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
