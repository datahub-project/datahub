import styled from 'styled-components';

import { CodeBlockStyleProps, CodeBlockVariant } from '@components/components/CodeBlock/types';
import { radius, spacing, typography } from '@components/theme';
import { getStatusColors } from '@components/theme/utils';

export const CodeBlockRoot = styled.div({
    display: 'flex',
    flexDirection: 'column' as const,
    width: '100%',
});

export const CodeBlockContainer = styled.div<{
    $variant: CodeBlockVariant;
    $editable?: boolean;
    $isInvalid?: boolean;
    $hasWarning?: boolean;
}>(({ theme, $variant, $editable, $isInvalid, $hasWarning }) => {
    const statusBorder =
        $variant === 'card' && ($isInvalid || $hasWarning)
            ? getStatusColors(false, $hasWarning ? 'warning' : undefined, $isInvalid, theme.colors)
            : undefined;

    return {
        display: 'flex',
        flexDirection: 'column' as const,
        width: '100%',
        overflow: 'hidden',
        background: theme.colors.bg,
        ...($variant === 'card'
            ? {
                  border: `1px solid ${statusBorder ?? theme.colors.border}`,
                  boxShadow: theme.colors.shadowSm,
                  borderRadius: radius.lg,
              }
            : {
                  border: 'none',
                  boxShadow: 'none',
                  borderRadius: 0,
                  background: 'transparent',
              }),
        ...($editable && $variant === 'card' && !$isInvalid && !$hasWarning
            ? {
                  '&:focus-within': {
                      borderColor: theme.colors.borderInputFocus,
                      boxShadow: theme.colors.shadowFocus,
                  },
              }
            : {}),
    };
});

const statusMessageStyles = {
    marginTop: spacing.xxsm,
    fontSize: typography.fontSizes.sm,
    fontFamily: typography.fonts.body,
    lineHeight: typography.lineHeights.sm,
};

export const CodeBlockErrorMessage = styled.div(({ theme }) => ({
    ...statusMessageStyles,
    color: theme.colors.textError,
}));

export const CodeBlockWarningMessage = styled.div(({ theme }) => ({
    ...statusMessageStyles,
    color: theme.colors.textWarning,
}));

export const CodeBlockIssueList = styled.ul({
    margin: `${spacing.xxsm} 0 0`,
    paddingLeft: spacing.md,
    listStyleType: 'disc',
});

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
    ({ theme, $variant, $overflow, $maxHeight, $clickable, $editable }) => ({
        overflowX: $overflow === 'hidden' ? 'hidden' : 'auto',
        overflowY: $overflow === 'hidden' ? 'hidden' : 'auto',
        maxHeight: $maxHeight,
        background: $variant === 'card' ? theme.colors.bg : 'transparent',
        cursor: ($clickable && 'pointer') || ($editable && 'text') || undefined,

        '& pre': {
            margin: '0 !important',
            padding: `${spacing.sm} !important`,
            background: 'transparent !important',
            border: 'none !important',
            borderRadius: '0 !important',
            overflow: 'visible !important',
            fontFamily: `${typography.fonts.mono} !important`,
            fontSize: `${typography.fontSizes.sm} !important`,
            fontWeight: `${typography.fontWeights.medium} !important`,
            lineHeight: '1.5 !important',
        },

        '& code, & span': {
            fontFamily: `${typography.fonts.mono} !important`,
            fontWeight: `${typography.fontWeights.medium} !important`,
            background: 'transparent !important',
        },
    }),
);

export const CodeBlockEditorFrame = styled.div({
    position: 'relative',
    width: '100%',
});

export const CodeBlockHighlightLayer = styled.div<{ $scroll?: boolean }>(({ $scroll }) => ({
    position: 'absolute',
    inset: 0,
    overflow: $scroll ? 'auto' : 'hidden',
    pointerEvents: 'none',
    ...($scroll
        ? {
              scrollbarWidth: 'none',
              msOverflowStyle: 'none',
              '&::-webkit-scrollbar': {
                  display: 'none',
              },
          }
        : {}),
}));

export const CodeBlockTextarea = styled.textarea<{
    $wrap: boolean;
    $disabled?: boolean;
    $maxHeight?: number | string;
    $scroll?: boolean;
}>(({ theme, $wrap, $disabled, $maxHeight, $scroll }) => ({
    position: 'relative',
    zIndex: 1,
    display: 'block',
    width: '100%',
    minHeight: '120px',
    boxSizing: 'border-box',
    margin: 0,
    border: 0,
    resize: 'none',
    outline: 'none',
    overflow: $scroll ? 'auto' : 'hidden',
    maxHeight: $maxHeight,
    overscrollBehavior: $scroll ? 'contain' : undefined,
    background: 'transparent',
    color: 'transparent',
    caretColor: theme.colors.text,
    padding: spacing.sm,
    fontFamily: typography.fonts.mono,
    fontSize: typography.fontSizes.sm,
    lineHeight: 1.5,
    whiteSpace: $wrap ? 'pre-wrap' : 'pre',
    wordBreak: $wrap ? 'break-word' : 'normal',
    tabSize: 2,
    cursor: $disabled ? 'not-allowed' : 'text',
    opacity: $disabled ? 0.7 : 1,
}));

export const CodeBlockPlaceholder = styled.div(({ theme }) => ({
    position: 'absolute',
    top: spacing.sm,
    left: spacing.sm,
    right: spacing.sm,
    zIndex: 2,
    pointerEvents: 'none',
    color: theme.colors.textPlaceholder,
    fontFamily: typography.fonts.mono,
    fontSize: typography.fontSizes.sm,
    lineHeight: 1.5,
}));

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

/** Compact empty-state body (e.g. "No generated answer yet.") inside CodeBlock chrome. */
export const CodeBlockEmptyBody = styled.div(({ theme }) => ({
    padding: spacing.sm,
    color: theme.colors.textSecondary,
    fontFamily: typography.fonts.body,
    fontSize: typography.fontSizes.sm,
    lineHeight: typography.lineHeights.sm,
}));

export const LanguageSelectWrapper = styled.div({
    minWidth: '140px',
    maxWidth: '220px',
});
