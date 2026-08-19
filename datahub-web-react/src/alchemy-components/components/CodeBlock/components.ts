import styled from 'styled-components';

import { CodeBlockStyleProps, CodeBlockVariant } from '@components/components/CodeBlock/types';
import { radius, spacing, typography } from '@components/theme';

export const CodeBlockContainer = styled.div<{ $variant: CodeBlockVariant }>(({ theme, $variant }) => ({
    display: 'flex',
    flexDirection: 'column' as const,
    width: '100%',
    overflow: 'hidden',
    background: theme.colors.bg,
    ...($variant === 'card'
        ? {
              border: `1px solid ${theme.colors.border}`,
              boxShadow: theme.colors.shadowSm,
              borderRadius: radius.lg,
          }
        : {
              border: 'none',
              boxShadow: 'none',
              borderRadius: 0,
              background: 'transparent',
          }),
}));

export const CodeBlockHeader = styled.div(({ theme }) => ({
    display: 'flex',
    flexDirection: 'row' as const,
    justifyContent: 'space-between',
    alignItems: 'center',
    gap: spacing.xsm,
    padding: `${spacing.xsm} ${spacing.sm}`,
    minHeight: '40px',
    background: theme.colors.bgSurface,
    borderBottom: `1px solid ${theme.colors.border}`,
}));

export const HeaderLeft = styled.div({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xsm,
    minWidth: 0,
    flex: 1,
});

export const HeaderRight = styled.div({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xxsm,
    flexShrink: 0,
});

export const LanguageLabel = styled.span(({ theme }) => ({
    fontFamily: typography.fonts.body,
    fontWeight: typography.fontWeights.bold,
    fontSize: typography.fontSizes.sm,
    lineHeight: typography.lineHeights.xs,
    color: theme.colors.text,
    whiteSpace: 'nowrap' as const,
}));

export const CodeBlockContent = styled.div<CodeBlockStyleProps>(
    ({ theme, $variant, $overflow, $maxHeight, $clickable }) => ({
        overflowX: $overflow === 'hidden' ? 'hidden' : 'auto',
        overflowY: $overflow === 'hidden' ? 'hidden' : 'auto',
        maxHeight: $maxHeight,
        background: $variant === 'card' ? theme.colors.bg : 'transparent',
        cursor: $clickable ? 'pointer' : undefined,

        '& pre': {
            margin: '0 !important',
            padding: `${spacing.sm} !important`,
            background: 'transparent !important',
            border: 'none !important',
            borderRadius: '0 !important',
            overflow: 'visible !important',
            fontFamily: `${typography.fonts.mono} !important`,
            fontSize: `${typography.fontSizes.sm} !important`,
            lineHeight: '1.5 !important',
        },

        '& code, & span': {
            fontFamily: `${typography.fonts.mono} !important`,
            background: 'transparent !important',
        },
    }),
);

export const TruncatedBanner = styled.div(({ theme }) => ({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xsm,
    padding: spacing.xsm,
    margin: `${spacing.sm} ${spacing.md}`,
    background: theme.colors.bgSurfaceWarning,
    borderRadius: radius.md,
    fontFamily: typography.fonts.body,
    fontSize: typography.fontSizes.md,
    lineHeight: typography.lineHeights.sm,
    color: theme.colors.textOnSurfaceWarning,
}));

export const LanguageSelectWrapper = styled.div({
    minWidth: '140px',
    maxWidth: '220px',
});
