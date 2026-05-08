import styled from 'styled-components';

import { AlertVariant } from '@components/components/Alert/types';

import { radius, spacing } from '@src/alchemy-components/theme';

const VARIANT_THEME_MAP: Record<AlertVariant, { bg: string; text: string; icon: string }> = {
    success: { bg: 'bgSurfaceSuccess', text: 'textSuccess', icon: 'iconSuccess' },
    error: { bg: 'bgSurfaceError', text: 'textError', icon: 'iconError' },
    warning: { bg: 'bgSurfaceWarning', text: 'textWarning', icon: 'iconWarning' },
    info: { bg: 'bgSurfaceInfo', text: 'textInformation', icon: 'iconInformation' },
    brand: { bg: 'bgSurfaceBrand', text: 'textBrand', icon: 'iconBrand' },
};

export const AlertContainer = styled.div<{ $variant: AlertVariant }>(({ $variant, theme }) => {
    const tokens = VARIANT_THEME_MAP[$variant];
    return {
        display: 'flex',
        alignItems: 'flex-start',
        gap: spacing.sm,
        padding: `${spacing.sm} ${spacing.md} ${spacing.sm} ${spacing.md}`,
        borderRadius: radius.lg,
        backgroundColor: theme.colors[tokens.bg],
        color: theme.colors[tokens.text],
    };
});

export const AlertIconWrapper = styled.span<{ $variant: AlertVariant }>(({ $variant, theme }) => {
    const tokens = VARIANT_THEME_MAP[$variant];
    return {
        display: 'inline-flex',
        alignItems: 'center',
        flexShrink: 0,
        paddingTop: '1px',
        color: theme.colors[tokens.icon],
    };
});

export const AlertContent = styled.div<{ $variant: AlertVariant }>(({ $variant, theme }) => {
    const tokens = VARIANT_THEME_MAP[$variant];
    return {
        display: 'flex',
        flexDirection: 'column',
        flex: 1,
        minWidth: 0,
        color: theme.colors[tokens.text],
    };
});

export const AlertActions = styled.div({
    display: 'flex',
    alignItems: 'center',
    flexShrink: 0,
    whiteSpace: 'nowrap',
});
