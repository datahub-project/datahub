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
 *   1. `platform.properties.logoUrl` when it's an absolute URL — the platform's own
 *      persisted logo from GMS (http(s)://, protocol-relative //, root-relative /, or data:).
 *   2. `PLATFORM_URN_TO_LOGO[platform.urn]` — the app's bundled logo map, imported through
 *      Vite so the emitted URL is always root-anchored / hashed.
 *   3. `platform.properties.logoUrl` as a bare-relative path fallback (last resort — will
 *      only resolve correctly if the current route matches the app root).
 *
 * GMS emits some built-in platform logos as bare-relative paths (e.g. "assets/platforms/x.png").
 * A bare-relative `src` resolves against the current URL, so on a route like
 * `/entity/urn:.../Documentation` the browser fetches
 * `/entity/urn:.../Documentation/assets/platforms/x.png`, which the SPA serves as
 * `text/html` (index.html fallback) and the <img> can't decode. Preferring the bundled
 * logo whenever the server URL isn't absolute avoids that trap for every external platform.
 *
 * Modeled after `PluginLogo` so the visual treatment matches the AI plugins surface — no
 * `ColorThief`-derived background tint, just the raw glyph.
 */
const ABSOLUTE_URL_PATTERN = /^(?:https?:)?\/\/|^\/|^data:/i;

export const DocumentSourceLogo: React.FC<DocumentSourceLogoProps> = ({ platform, size, fallback = null }) => {
    const [hasError, setHasError] = useState(false);
    const serverUrl = platform?.properties?.logoUrl;
    const bundledUrl = platform?.urn ? PLATFORM_URN_TO_LOGO[platform.urn] : undefined;
    const serverUrlIsAbsolute = !!serverUrl && ABSOLUTE_URL_PATTERN.test(serverUrl);
    const logoUrl = serverUrlIsAbsolute ? serverUrl : bundledUrl || serverUrl || undefined;

    if (!logoUrl || hasError) {
        return <>{fallback}</>;
    }

    return <LogoImage src={logoUrl} alt={platform?.name || ''} $size={size} onError={() => setHasError(true)} />;
};
