import { colors, typography } from '@components/theme';

export const INPUT_MAX_HEIGHT = '40px';

export const formLabelTextStyles = {
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes.md,
    color: `${(props) => props.theme.styles['primary-color']}`,
};

export const inputValueTextStyles = (size = 'md') => ({
    fontFamily: typography.fonts.body,
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes[size],
    color: `${(props) => props.theme.styles['primary-color-dark']}`,
});

export const inputPlaceholderTextStyles = {
    fontFamily: typography.fonts.body,
    fontWeight: typography.fontWeights.normal,
    fontSize: typography.fontSizes.md,
    color: `${(props) => props.theme.styles['primary-color-light']}`,
};
