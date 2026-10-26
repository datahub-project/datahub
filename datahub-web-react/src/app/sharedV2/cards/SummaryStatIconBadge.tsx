import type { Icon } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

export type SummaryStatTone = 'brand' | 'success' | 'info' | 'warning' | 'neutral';

const TONE_BG: Record<
    SummaryStatTone,
    'bgSurfaceBrand' | 'bgSurfaceSuccess' | 'bgSurfaceInfo' | 'bgSurfaceWarning' | 'bgSurface'
> = {
    brand: 'bgSurfaceBrand',
    success: 'bgSurfaceSuccess',
    info: 'bgSurfaceInfo',
    warning: 'bgSurfaceWarning',
    neutral: 'bgSurface',
};

const TONE_FG: Record<
    SummaryStatTone,
    'iconBrand' | 'iconSuccess' | 'iconInformation' | 'iconWarning' | 'textSecondary'
> = {
    brand: 'iconBrand',
    success: 'iconSuccess',
    info: 'iconInformation',
    warning: 'iconWarning',
    neutral: 'textSecondary',
};

const IconBadge = styled.div<{ $tone: SummaryStatTone }>`
    width: 32px;
    height: 32px;
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    background: ${(props) => props.theme.colors[TONE_BG[props.$tone]]};
    color: ${(props) => props.theme.colors[TONE_FG[props.$tone]]};
`;

type Props = {
    icon: Icon;
    tone: SummaryStatTone;
};

/** Toned icon badge for browse-home summary stat cards (matches Documents home tiles). */
export function SummaryStatIconBadge({ icon: IconComponent, tone }: Props) {
    return (
        <IconBadge $tone={tone}>
            <IconComponent size={18} weight="regular" />
        </IconBadge>
    );
}
