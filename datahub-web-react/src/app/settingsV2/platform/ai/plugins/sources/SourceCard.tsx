import { Plug } from '@phosphor-icons/react';
import React, { useState } from 'react';
import styled from 'styled-components';

import { PluginSourceConfig } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources.types';
import { KNOWN_MCP_LOGOS } from '@app/settingsV2/platform/ai/plugins/utils/pluginLogoUtils';
import { Card, Text, colors } from '@src/alchemy-components';

// Map source name -> logo lookup key in KNOWN_MCP_LOGOS
const SOURCE_LOGO_DOMAINS: Record<string, string> = {
    github: 'github.com',
    dbt: 'dbt.com',
    snowflake: 'snowflake.com',
};

const LogoImage = styled.img`
    height: 28px;
    width: 28px;
    object-fit: contain;
`;

interface SourceCardProps {
    source: PluginSourceConfig;
    onSelect: (source: PluginSourceConfig) => void;
}

/**
 * Card component for the source selection grid (Step 1).
 * Renders the source logo, name, and description.
 */
export const SourceCard: React.FC<SourceCardProps> = ({ source, onSelect }) => {
    const [hasError, setHasError] = useState(false);

    const domain = SOURCE_LOGO_DOMAINS[source.name];
    const logoUrl = domain ? KNOWN_MCP_LOGOS[domain] : null;

    const icon =
        logoUrl && !hasError ? (
            <LogoImage src={logoUrl} alt={`${source.displayName} logo`} onError={() => setHasError(true)} />
        ) : (
            <Plug size={28} color={colors.gray[400]} />
        );

    return (
        <Card
            title={source.displayName}
            subTitle={<Text color="gray">{source.description}</Text>}
            icon={icon}
            width="100%"
            iconAlignment="horizontal"
            iconStyles={{ alignSelf: 'start' }}
            onClick={() => onSelect(source)}
            isCardClickable
            dataTestId={`source-card-${source.name}`}
        />
    );
};
