import { colors, radius, spacing, typography } from '@src/alchemy-components/theme';
import { IconAlignmentOptions } from '@src/alchemy-components/theme/config';
import styled from 'styled-components';

export const CardContainer = styled.div<{ hasButton?: boolean; width?: string; maxWidth?: string; height?: string }>(
    ({ hasButton, width, maxWidth, height }) => ({
        border: `1px solid ${colors.gray[100]}`,
        borderRadius: radius.lg,
        padding: spacing.md,
        display: 'flex',
        flex: `1 1 ${maxWidth}`,
        minWidth: '150px',
        boxShadow: '0px 1px 2px 0px rgba(33, 23, 95, 0.07)',
        backgroundColor: colors.white,
        flexDirection: 'column',
        gap: spacing.md,
        maxWidth,
        width,
        height,

        '&:hover': hasButton
            ? {
                  border: `1px solid ${colors.violet[500]}`,
                  cursor: 'pointer',
              }
            : {},
    }),
);

export const Header = styled.div<{ iconAlignment?: IconAlignmentOptions }>(({ iconAlignment }) => ({
    display: 'flex',
    flexDirection: iconAlignment === 'horizontal' ? 'row' : 'column',
    alignItems: iconAlignment === 'horizontal' ? 'center' : 'start',
    gap: spacing.sm,
    width: '100%',
}));

export const TitleContainer = styled.div({
    display: 'flex',
    flexDirection: 'column',
    gap: 2,
    width: '100%',
});

export const Title = styled.div<{ $isEmpty?: boolean }>(({ $isEmpty }) => ({
    fontSize: typography.fontSizes.lg,
    fontWeight: typography.fontWeights.bold,
    color: $isEmpty ? colors.gray[1800] : colors.gray[600],
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xsm,
}));

export const SubTitleContainer = styled.div({
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
});

export const SubTitle = styled.div({
    fontSize: typography.fontSizes.md,
    fontWeight: typography.fontWeights.normal,
    color: colors.gray[1700],
});
