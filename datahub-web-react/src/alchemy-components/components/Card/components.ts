import styled from 'styled-components';

import { CardSize } from '@components/components/Card/types';

import { radius, spacing, typography } from '@src/alchemy-components/theme';
import { IconAlignmentOptions } from '@src/alchemy-components/theme/config';

const SIZE_PADDING: Record<CardSize, string> = {
    sm: spacing.sm,
    md: spacing.md,
};

const SIZE_GAP: Record<CardSize, string> = {
    sm: spacing.xsm,
    md: spacing.md,
};

const TITLE_FONT_SIZE: Record<CardSize, string> = {
    sm: typography.fontSizes.sm,
    md: typography.fontSizes.lg,
};

export const CardContainer = styled.div<{
    isClickable?: boolean;
    width?: string;
    maxWidth?: string;
    height?: string;
    $size?: CardSize;
}>(({ isClickable, width, maxWidth, height, $size = 'md', theme }) => ({
    border: `1px solid ${theme.colors.border}`,
    borderRadius: radius.lg,
    padding: SIZE_PADDING[$size],
    display: 'flex',
    flex: `1 1 ${maxWidth}`,
    minWidth: '150px',
    boxShadow: theme.colors.shadowXs,
    backgroundColor: theme.colors.bg,
    flexDirection: 'column',
    gap: SIZE_GAP[$size],
    maxWidth,
    width,
    height,
    overflow: 'hidden',

    '&:hover': isClickable
        ? {
              border: `1px solid ${theme.colors.borderBrand}`,
              cursor: 'pointer',
          }
        : {},
}));

export const Header = styled.div<{
    iconAlignment?: IconAlignmentOptions;
    $size?: CardSize;
    $collapsible?: boolean;
}>(({ iconAlignment, $size = 'md', $collapsible }) => ({
    display: 'flex',
    flexDirection: iconAlignment === 'horizontal' ? 'row' : 'column',
    alignItems: iconAlignment === 'horizontal' ? 'center' : 'start',
    gap: $size === 'sm' ? spacing.xsm : spacing.sm,
    width: '100%',
    ...($collapsible
        ? {
              cursor: 'pointer',
              userSelect: 'none' as const,
          }
        : {}),
}));

export const TitleContainer = styled.div({
    display: 'flex',
    flexDirection: 'column',
    gap: 2,
    width: '100%',
    minWidth: 0,
});

export const Title = styled.div<{ $isEmpty?: boolean; $size?: CardSize }>(({ $isEmpty, $size = 'md', theme }) => ({
    fontSize: TITLE_FONT_SIZE[$size],
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

export const SubTitle = styled.div<{ $noOfSubtitleLines?: number; $size?: CardSize }>(
    ({ $noOfSubtitleLines, theme }) => ({
        fontSize: typography.fontSizes.md,
        fontWeight: typography.fontWeights.normal,
        color: theme.colors.textSecondary,
        lineHeight: 'normal',
        wordWrap: 'break-word',

        ...($noOfSubtitleLines
            ? {
                  display: '-webkit-box',
                  WebkitLineClamp: $noOfSubtitleLines,
                  WebkitBoxOrient: 'vertical',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
              }
            : {}),
    }),
);

export const ExpandButton = styled.button(({ theme }) => ({
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    flexShrink: 0,
    padding: 0,
    margin: 0,
    border: 'none',
    background: 'transparent',
    color: theme.colors.textSecondary,
    cursor: 'pointer',
    lineHeight: 0,
}));

export const CollapsibleBody = styled.div<{ $size?: CardSize }>(({ $size = 'md' }) => ({
    display: 'flex',
    flexDirection: 'column',
    gap: SIZE_GAP[$size],
    width: '100%',
}));
