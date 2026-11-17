import React from 'react';
import styled from 'styled-components/macro';

import { useCapabilitySummary } from '@app/ingestV2/shared/hooks/useCapabilitySummary';
import TestConnectionButton from '@app/ingestV2/source/builder/RecipeForm/TestConnection/TestConnectionButton';
import { SourceConfig } from '@app/ingestV2/source/builder/types';
import TestConnectionModal from '@app/ingestV2/source/multiStepBuilder/steps/step2ConnectionDetails/sections/recipeSection/recipeForm/components/TestConnection/TestConnectionModal';

import { IngestionSource } from '@types';

const TestConnectionWrapper = styled.div`
    display: flex;
    justify-content: flex-start;
`;

interface Props {
    type?: string;
    recipe: string;
    sourceConfigs?: SourceConfig;
    version?: string | null;
    selectedSource?: IngestionSource;
}

/** The Test Connection button, shown only when the source's connector supports it. */
export function TestConnectionSection({ type, recipe, sourceConfigs, version, selectedSource }: Props) {
    const { getConnectorsWithTestConnection } = useCapabilitySummary();

    if (!type || !getConnectorsWithTestConnection().has(type)) {
        return null;
    }

    return (
        <TestConnectionWrapper>
            <TestConnectionButton
                recipe={recipe}
                sourceConfigs={sourceConfigs}
                version={version}
                selectedSource={selectedSource}
                size="xs"
                textWeight="semiBold"
                renderModal={(props) => <TestConnectionModal {...props} />}
                hideIcon
            />
        </TestConnectionWrapper>
    );
}
