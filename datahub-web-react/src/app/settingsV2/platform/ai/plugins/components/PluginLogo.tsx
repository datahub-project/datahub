import { Plug } from '@phosphor-icons/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { getPluginLogoUrl } from '@app/settingsV2/platform/ai/plugins/utils/pluginLogoUtils';
import { colors } from '@src/alchemy-components';

const LogoImage = styled.img`
    height: 20px;
    width: 20px;
    object-fit: contain;
    background-color: transparent;
`;

type PluginLogoProps = {
    displayName: string;
    url: string | null;
};

/**
 * Renders a plugin logo with fallback to default plug icon.
 * Uses getPluginLogoUrl to resolve the logo from display name or URL.
 */
export const PluginLogo: React.FC<PluginLogoProps> = ({ displayName, url }) => {
    const [hasError, setHasError] = useState(false);
    const logoUrl = getPluginLogoUrl(displayName, url);

    if (!logoUrl || hasError) {
        return <Plug size={20} color={colors.gray[400]} />;
    }

    return <LogoImage src={logoUrl} alt="Plugin logo" onError={() => setHasError(true)} />;
};
