import { typography } from '@components/theme';

export const getHeaderTitleStyles = (variant) => {
    if (variant === 'sectionHeader') {
        return {
            fontSize: typography.fontSizes.lg,
            lineHeight: typography.lineHeights.lg,
        };
    }
    return {
        fontSize: typography.fontSizes['3xl'],
        lineHeight: typography.lineHeights['3xl'],
    };
};

export const getHeaderSubtitleStyles = (variant) => {
    if (variant === 'sectionHeader') {
        return {
            fontSize: typography.fontSizes.md,
            lineHeight: typography.lineHeights.md,
        };
    }
    return {
        fontSize: typography.fontSizes.lg,
        lineHeight: typography.lineHeights.lg,
    };
};
