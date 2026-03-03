import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useIngestionSources } from '@app/ingestV2/source/builder/useIngestionSources';
import { IngestionSourceFormStep, MultiStepSourceBuilderState } from '@app/ingestV2/source/multiStepBuilder/types';
import { getSourceConfigs } from '@app/ingestV2/source/utils';
import { useMultiStepContext } from '@app/sharedV2/forms/multiStepForm/MultiStepFormContext';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const INGESTION_SECURITY_URL = 'https://docs.datahub.com/docs/metadata-ingestion-security';

export function FormHeader() {
    const { state } = useMultiStepContext<MultiStepSourceBuilderState, IngestionSourceFormStep>();
    const { type } = state;
    const { ingestionSources } = useIngestionSources();
    const sourceConfigs = getSourceConfigs(ingestionSources, type as string);
    const sourceDisplayName = sourceConfigs?.displayName;

    return (
        <Wrapper>
            <Text weight="semiBold" size="lg">
                {sourceDisplayName} Connection Details
            </Text>
            <Text color="gray" type="span">
                Configure how DataHub connects to {sourceDisplayName}.{' '}
                <Text color="gray" type="span" size="sm">
                    <a href={INGESTION_SECURITY_URL} target="_blank" rel="noreferrer">
                        Learn more about keeping credentials in your environment.
                    </a>
                </Text>
            </Text>
        </Wrapper>
    );
}
