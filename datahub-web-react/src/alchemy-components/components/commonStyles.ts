import { colors, typography } from '@components/theme';

export const INPUT_MAX_HEIGHT = '40px';

export const formLabelTextStyles = {
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes.md,
    color: `${colors.primary[600]}`,
};

export const inputValueTextStyles = (size = 'md') => ({
    fontFamily: typography.fonts.body,
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes[size],
    color: `${colors.primary[700]}`,
});

export const inputPlaceholderTextStyles = {
    fontFamily: typography.fonts.body,
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes.md,
    color: `${colors.primary[400]}`,
};
