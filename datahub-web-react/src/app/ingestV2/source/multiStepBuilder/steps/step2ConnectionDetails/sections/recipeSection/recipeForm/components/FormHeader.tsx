import { Text } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('ingestion.sourceBuilder');
    const { state } = useMultiStepContext<MultiStepSourceBuilderState, IngestionSourceFormStep>();
    const { type } = state;
    const { ingestionSources } = useIngestionSources();
    const sourceConfigs = getSourceConfigs(ingestionSources, type as string);
    const sourceDisplayName = sourceConfigs?.displayName;

    return (
        <Wrapper>
            <Text weight="semiBold" size="lg">
                {t('multiStep.recipeForm.connectionDetailsTitle', { sourceDisplayName })}
            </Text>
            <Text type="span">
                <Trans
                    t={t}
                    i18nKey="multiStep.recipeForm.connectionDetailsDescription"
                    values={{ sourceDisplayName }}
                    components={{
                        securityText: <Text type="span" size="sm" />,
                        securityLink: (
                            <a href={INGESTION_SECURITY_URL} target="_blank" rel="noreferrer">
                                {null}
                            </a>
                        ),
                    }}
                />
            </Text>
        </Wrapper>
    );
}
