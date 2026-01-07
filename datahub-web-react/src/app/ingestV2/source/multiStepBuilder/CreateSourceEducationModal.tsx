import { Modal, Text } from '@components';
import { Image } from 'antd';
import React from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { EXTERNAL_SOURCE_REDIRECT_URL } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/utils';
import { INGESTION_SELECT_SOURCE_ID } from '@app/onboarding/config/IngestionOnboardingConfig';
import useHasSeenEducationStep from '@providers/hooks/useHasSeenEducationStep';
import useUpdateEducationStep from '@providers/hooks/useUpdateEducationStep';

import displayImage from '@images/create-source-modal.jpg';

const ContentContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

const StyledImage = styled(Image)`
    border-radius: 8px;
    box-shadow: 0 0 24px 0 rgba(83, 63, 209, 0.1);
    border: 0.3px solid #f3f5f8;
`;

const ItalicsText = styled(Text)`
    font-style: italic;
`;

export default function CreateSourceEducationModal() {
    const hasSeenStep = useHasSeenEducationStep(INGESTION_SELECT_SOURCE_ID);
    const { updateEducationStep } = useUpdateEducationStep();

    function handleClose() {
        updateEducationStep(INGESTION_SELECT_SOURCE_ID);
        analytics.event({
            type: EventType.CloseCreateSourceEducationModalEvent,
        });
    }

    return (
        <Modal
            open={!hasSeenStep}
            onCancel={handleClose}
            centered
            buttons={[
                {
                    variant: 'filled',
                    onClick: handleClose,
                    buttonDataTestId: 'modal-confirm-button',
                    text: `Let's Go!`,
                    color: 'primary',
                },
            ]}
            title="Connect Your Data"
            maskStyle={{ backgroundColor: 'rgba(0, 0, 0, 0.1)', backdropFilter: 'blur(2px)' }}
            bodyStyle={{ padding: '16px' }}
        >
            <ContentContainer>
                <Text color="gray" colorLevel={600} size="lg" weight="bold">
                    Set Up Your First Source in Minutes
                </Text>
                <Text color="gray" colorLevel={1700} weight="medium">
                    We&apos;ll guide you through each step - Ask DataHub is here to help with configuration and
                    troubleshooting as you go.
                </Text>
                <Text color="gray" colorLevel={1700} weight="medium">
                    <Text weight="bold" type="span">
                        Pro Tip:{' '}
                    </Text>
                    Start with your data warehouse (Snowflake, BigQuery, Databricks) as a strong foundation.
                </Text>
                <Text color="gray" colorLevel={1700} weight="medium">
                    Then, connect upstream platforms where data originates or downstream tools where it&apos;s consumed,
                    and let DataHub automatically map lineage between them.
                </Text>
                <StyledImage src={displayImage} preview={false} />
                <ItalicsText color="gray" size="sm">
                    Prefer to keep credentials local? Get started with{' '}
                    <a href={EXTERNAL_SOURCE_REDIRECT_URL} target="_blank" rel="noreferrer">
                        DataHub&apos;s Ingestion CLI
                    </a>
                </ItalicsText>
            </ContentContainer>
        </Modal>
    );
}
