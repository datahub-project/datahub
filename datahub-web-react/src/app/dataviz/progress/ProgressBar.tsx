import React from 'react';
import styled, { DefaultTheme } from 'styled-components';

export type ProgressBarVariant = 'brand' | 'success' | 'warning' | 'error';

/**
 * When `'auto'`, the variant is derived from value:
 *   <50 → error (red), <75 → warning (yellow), >=75 → success (green).
 *
 * Brand (violet) is never used in `'auto'` — pass `variant="brand"` (or omit
 * `variant`, since brand is the default) to opt into the violet gradient.
 */
export type ProgressBarVariantOrAuto = ProgressBarVariant | 'auto';

type Props = {
    /** Progress value, 0–100. */
    value: number;
    /** Color variant for the fill gradient. Defaults to 'brand'. */
    variant?: ProgressBarVariantOrAuto;
    /** Optional left-aligned label rendered above the bar. */
    leftLabel?: React.ReactNode;
    /** Optional right-aligned label rendered above the bar. */
    rightLabel?: React.ReactNode;
    /** Optional helper text rendered below the bar. */
    subtext?: React.ReactNode;
    /** Bar thickness in pixels. Defaults to 6. */
    thickness?: number;
    /** Accessible label for the progress bar. */
    ariaLabel?: string;
};

function gradientStops(theme: DefaultTheme, variant: ProgressBarVariant): [string, string] {
    switch (variant) {
        case 'success':
            return [theme.colors.iconSuccess, theme.colors.borderSuccess];
        case 'warning':
            return [theme.colors.iconWarning, theme.colors.borderWarning];
        case 'error':
            return [theme.colors.iconError, theme.colors.borderError];
        case 'brand':
        default:
            return [theme.colors.iconBrand, theme.colors.borderBrand];
    }
}

function resolveVariant(variant: ProgressBarVariantOrAuto, value: number): ProgressBarVariant {
    if (variant !== 'auto') return variant;
    if (value < 50) return 'error';
    if (value < 75) return 'warning';
    return 'success';
}

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 2px;
    width: 100%;
`;

const StatusRow = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    line-height: 15px;
    font-weight: 700;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const Track = styled.div<{ $thickness: number }>`
    width: 100%;
    height: ${({ $thickness }) => $thickness}px;
    border-radius: 200px;
    background: ${(props) => props.theme.colors.bgSurface};
    overflow: hidden;
`;

const Fill = styled.div<{ $pct: number; $variant: ProgressBarVariant }>`
    height: 100%;
    width: ${({ $pct }) => $pct}%;
    background: ${({ theme, $variant }) => {
        const [start, end] = gradientStops(theme, $variant);
        return `linear-gradient(270deg, ${start} 0%, ${end} 100%)`;
    }};
    border-radius: 200px;
    transition: width 0.4s ease;
`;

const Subtext = styled.div`
    font-size: 12px;
    line-height: 15px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textSecondary};
`;

export function ProgressBar({
    value,
    variant = 'brand',
    leftLabel,
    rightLabel,
    subtext,
    thickness = 6,
    ariaLabel,
}: Props) {
    const pct = Math.max(0, Math.min(100, value));
    const resolvedVariant = resolveVariant(variant, pct);
    const showStatusRow = leftLabel !== undefined || rightLabel !== undefined;

    return (
        <Container>
            {showStatusRow && (
                <StatusRow>
                    <span>{leftLabel}</span>
                    <span>{rightLabel}</span>
                </StatusRow>
            )}
            <Track
                $thickness={thickness}
                role="progressbar"
                aria-label={ariaLabel}
                aria-valuenow={Math.round(pct)}
                aria-valuemin={0}
                aria-valuemax={100}
            >
                <Fill $pct={pct} $variant={resolvedVariant} />
            </Track>
            {subtext !== undefined && <Subtext>{subtext}</Subtext>}
        </Container>
    );
}
