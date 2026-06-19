import styled from 'styled-components';

import { AlertVariant } from '@components/components/Alert/types';

import { radius, spacing, typography } from '@src/alchemy-components/theme';

const VARIANT_THEME_MAP: Record<AlertVariant, { bg: string; text: string; icon: string }> = {
    success: { bg: 'bgSurfaceSuccess', text: 'textSuccess', icon: 'iconSuccess' },
    error: { bg: 'bgSurfaceError', text: 'textError', icon: 'iconError' },
    warning: { bg: 'bgSurfaceWarning', text: 'textWarning', icon: 'iconWarning' },
    info: { bg: 'bgSurfaceInfo', text: 'textInformation', icon: 'iconInformation' },
    brand: { bg: 'bgSurfaceBrand', text: 'textBrand', icon: 'iconBrand' },
    unknown: { bg: 'bgSurface', text: 'textSecondary', icon: 'icon' },
};

// Outer container is a vertical stack: header row, then description, errorMessage,
// inline actions. Description and errorMessage span the full width — they are no
// longer constrained by what's on the right side of the header (close + topRight action).
export const AlertContainer = styled.div<{ $variant: AlertVariant; $hasClose?: boolean }>(
    ({ $variant, $hasClose, theme }) => {
        const tokens = VARIANT_THEME_MAP[$variant];
        return {
            display: 'flex',
            flexDirection: 'column',
            padding: `${spacing.sm} ${$hasClose ? spacing.xsm : spacing.md} ${spacing.sm} ${spacing.md}`,
            borderRadius: radius.lg,
            backgroundColor: theme.colors[tokens.bg],
            color: theme.colors[tokens.text],
        };
    },
);

// Body content (description, errorMessage, inline action) lives in this wrapper
// so it can be indented to align flush with the title text rather than the
// icon's left edge. The indent equals icon width (20px) + header gap
// (spacing.xsm = 8px) = 28px.
//
// The negative marginTop tightens the visual gap to the title above —
// without it the line-height leading on both elements (~5px each) leaves
// the description feeling too far below the title.
export const AlertBody = styled.div({
    display: 'flex',
    flexDirection: 'column',
    gap: 4,
    paddingLeft: '28px',
    marginTop: '-4px',
});

// Top header row: left group (icon + title) on the left, right group (topRight
// action + close button) on the right.
export const AlertHeader = styled.div({
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    gap: spacing.xsm,
});

// Left half of the header: icon + title, sitting on the same baseline.
export const AlertHeaderLeft = styled.div({
    display: 'flex',
    alignItems: 'center',
    gap: spacing.xsm,
    flex: 1,
    minWidth: 0,
});

// Right half of the header: optional topRight action followed by the close button.
// Buttons keep their normal padding here — they read as proper buttons next to the X.
export const AlertHeaderRight = styled.div({
    display: 'flex',
    alignItems: 'center',
    flexShrink: 0,
    whiteSpace: 'nowrap',
});

export const AlertIconWrapper = styled.span<{ $variant: AlertVariant }>(({ $variant, theme }) => {
    const tokens = VARIANT_THEME_MAP[$variant];
    return {
        display: 'inline-flex',
        alignItems: 'center',
        flexShrink: 0,
        color: theme.colors[tokens.icon],
    };
});

// Wrapper for the optional inline action that lives below the description.
// We pull the wrapper left by the alchemy Button's default md horizontal
// padding (12px) so the button label sits flush-left with the description
// text above it, while the button itself keeps its normal hit-target padding.
export const AlertActions = styled.div({
    display: 'flex',
    alignItems: 'center',
    flexShrink: 0,
    whiteSpace: 'nowrap',
    marginLeft: '-12px',
});

export const AlertErrorMessage = styled.div(({ theme }) => ({
    backgroundColor: theme.colors.bg,
    borderRadius: radius.md,
    padding: `${spacing.xxsm} ${spacing.xsm}`,
    fontFamily: typography.fonts.mono,
    fontSize: '13px',
    overflowWrap: 'break-word' as const,
    wordBreak: 'break-all' as const,
}));
