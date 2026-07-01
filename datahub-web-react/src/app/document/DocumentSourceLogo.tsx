import React, { useState } from 'react';
import styled from 'styled-components';

import { PLATFORM_URN_TO_LOGO } from '@app/ingestV2/source/builder/constants';

import { DataPlatform } from '@types';

const LogoImage = styled.img<{ $size: number }>`
    height: ${(props) => props.$size}px;
    width: ${(props) => props.$size}px;
    object-fit: contain;
    background-color: transparent;
`;

interface DocumentSourceLogoProps {
    platform: DataPlatform | null | undefined;
    size: number;
    /** Optional fallback rendered when no logo URL can be resolved (rare for known platforms). */
    fallback?: React.ReactNode;
}

/**
 * Resolves the logo URL for a source platform and renders it as a plain transparent <img>.
 *
 * Resolution order:
 *   1. `platform.properties.logoUrl` — the platform's own persisted logo from GMS
 *   2. `PLATFORM_URN_TO_LOGO[platform.urn]` — the app's bundled logo map (used everywhere else
 *      in the app where logos need to render even when GMS hasn't ingested a logoUrl)
 *
 * Modeled after `PluginLogo` so the visual treatment matches the AI plugins surface — no
 * `ColorThief`-derived background tint, just the raw glyph.
 */
export const DocumentSourceLogo: React.FC<DocumentSourceLogoProps> = ({ platform, size, fallback = null }) => {
    const [hasError, setHasError] = useState(false);
    const logoUrl = platform?.properties?.logoUrl || (platform?.urn ? PLATFORM_URN_TO_LOGO[platform.urn] : undefined);

    if (!logoUrl || hasError) {
        return <>{fallback}</>;
    }

    return <LogoImage src={logoUrl} alt={platform?.name || ''} $size={size} onError={() => setHasError(true)} />;
};
