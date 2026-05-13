import styled from 'styled-components';

import { EmptyStateSize } from '@components/components/EmptyState/types';

import { spacing } from '@src/alchemy-components/theme';

const SIZE_GAP: Record<EmptyStateSize, string> = {
    sm: spacing.sm,
    default: spacing.md,
    lg: spacing.lg,
};

const SIZE_PADDING: Record<EmptyStateSize, string> = {
    sm: `${spacing.lg} ${spacing.md}`,
    default: `${spacing.xxlg} ${spacing.lg}`,
    lg: `${spacing.xxlg} ${spacing.xlg}`,
};

export const Container = styled.div<{ $size: EmptyStateSize }>(({ $size }) => ({
    display: 'flex',
    flexDirection: 'column' as const,
    alignItems: 'center',
    justifyContent: 'center',
    textAlign: 'center' as const,
    padding: SIZE_PADDING[$size],
    gap: SIZE_GAP[$size],
}));

export const IconWrapper = styled.div(({ theme }) => ({
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    color: theme.colors.textTertiary,
}));

export const TitleText = styled.div(({ theme }) => ({
    color: theme.colors.text,
}));

export const DescriptionText = styled.div(({ theme }) => ({
    color: theme.colors.textSecondary,
}));

export const TextWrapper = styled.div({
    display: 'flex',
    flexDirection: 'column' as const,
    alignItems: 'center',
});

export const ActionsWrapper = styled.div({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.sm,
    marginTop: spacing.xsm,
});
