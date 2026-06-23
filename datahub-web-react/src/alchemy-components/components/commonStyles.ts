import { typography } from '@components/theme';

export const INPUT_MAX_HEIGHT = '40px';

export const formLabelTextStyles = {
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes.md,
};

export const inputValueTextStyles = (size = 'md') => ({
    fontFamily: typography.fonts.body,
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes[size],
});

export const inputPlaceholderTextStyles = {
    fontFamily: typography.fonts.body,
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes.md,
};
