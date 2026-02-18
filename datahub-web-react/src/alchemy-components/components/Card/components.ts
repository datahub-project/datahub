import styled from 'styled-components';

import { radius, spacing, typography } from '@src/alchemy-components/theme';
import { IconAlignmentOptions } from '@src/alchemy-components/theme/config';

export const CardContainer = styled.div<{ isClickable?: boolean; width?: string; maxWidth?: string; height?: string }>(
    ({ isClickable, width, maxWidth, height, theme }) => ({
        border: `1px solid ${theme.colors.border}`,
        borderRadius: radius.lg,
        padding: spacing.md,
        display: 'flex',
        flex: `1 1 ${maxWidth}`,
        minWidth: '150px',
        boxShadow: '0px 1px 2px 0px rgba(33, 23, 95, 0.07)',
        backgroundColor: theme.colors.bg,
        flexDirection: 'column',
        gap: spacing.md,
        maxWidth,
        width,
        height,

        '&:hover': isClickable
            ? {
                  border: `1px solid ${theme.styles['primary-color']}`,
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

export const Title = styled.div<{ $isEmpty?: boolean }>(({ $isEmpty, theme }) => ({
    fontSize: typography.fontSizes.lg,
    fontWeight: typography.fontWeights.bold,
    color: $isEmpty ? theme.colors.textTertiary : theme.colors.text,
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xsm,
    lineHeight: 'normal',
}));

export const SubTitleContainer = styled.div({
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
});

export const SubTitle = styled.div<{ $noOfSubtitleLines?: number }>(({ $noOfSubtitleLines, theme }) => ({
    fontSize: typography.fontSizes.md,
    fontWeight: typography.fontWeights.normal,
    color: theme.colors.textSecondary,
    lineHeight: 'normal',

    ...($noOfSubtitleLines
        ? {
              display: '-webkit-box',
              WebkitLineClamp: $noOfSubtitleLines,
              WebkitBoxOrient: 'vertical',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
          }
        : {}),
}));
