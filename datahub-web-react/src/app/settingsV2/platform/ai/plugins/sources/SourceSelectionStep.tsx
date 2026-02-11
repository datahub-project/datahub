import React from 'react';
import styled from 'styled-components';

import { SourceCard } from '@app/settingsV2/platform/ai/plugins/sources/SourceCard';
import { PLUGIN_SOURCES } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources';
import { PluginSourceConfig } from '@app/settingsV2/platform/ai/plugins/sources/pluginSources.types';

const Grid = styled.div`
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
`;

interface SourceSelectionStepProps {
    onSelect: (source: PluginSourceConfig) => void;
}

/**
 * Step 1 of the plugin creation wizard.
 * Renders a grid of source cards for the user to choose from.
 */
export const SourceSelectionStep: React.FC<SourceSelectionStepProps> = ({ onSelect }) => {
    return (
        <Grid data-testid="source-selection-step">
            {PLUGIN_SOURCES.map((source) => (
                <SourceCard key={source.name} source={source} onSelect={onSelect} />
            ))}
        </Grid>
    );
};
