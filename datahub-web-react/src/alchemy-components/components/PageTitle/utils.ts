import { typography } from '@components/theme';

export const getHeaderTitleStyles = (variant) => {
    if (variant === 'sectionHeader') {
        return {
            fontSize: typography.fontSizes.lg,
        };
    }
    return {
        fontSize: typography.fontSizes['3xl'],
    };
};

export const getHeaderSubtitleStyles = (variant) => {
    if (variant === 'sectionHeader') {
        return {
            fontSize: typography.fontSizes.md,
        };
    }
    return {
        fontSize: typography.fontSizes.lg,
    };
};
