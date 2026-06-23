import { Modal, Text } from '@components';
import { Image } from 'antd';
import React, { useContext } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled, { useTheme } from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { EXTERNAL_SOURCE_REDIRECT_URL } from '@app/ingestV2/source/multiStepBuilder/steps/step1SelectSource/utils';
import OnboardingContext from '@app/onboarding/OnboardingContext';
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
    box-shadow: ${(props) => props.theme.colors.shadowLg};
    border: 0.3px solid ${(props) => props.theme.colors.border};
`;

const ItalicsText = styled(Text)`
    font-style: italic;
`;

const BACKDROP_FILTER = 'blur(2px)';

export default function CreateSourceEducationModal() {
    const { t } = useTranslation('ingestion.sourceBuilder');
    const theme = useTheme();
    const hasSeenStep = useHasSeenEducationStep(INGESTION_SELECT_SOURCE_ID);
    const { updateEducationStep } = useUpdateEducationStep();
    const { isTourOpen, setIsTourOpen } = useContext(OnboardingContext);

    function handleClose() {
        if (isTourOpen) {
            setIsTourOpen(false);
        } else {
            updateEducationStep(INGESTION_SELECT_SOURCE_ID);
            analytics.event({
                type: EventType.CloseCreateSourceEducationModalEvent,
            });
        }
    }

    return (
        <Modal
            open={!hasSeenStep || isTourOpen}
            onCancel={handleClose}
            centered
            buttons={[
                {
                    variant: 'filled',
                    onClick: handleClose,
                    buttonDataTestId: 'modal-confirm-button',
                    text: t('multiStep.education.cta'),
                    color: 'primary',
                },
            ]}
            title={t('multiStep.education.title')}
            maskStyle={{ backgroundColor: theme.colors.overlayMedium, backdropFilter: BACKDROP_FILTER }}
            bodyStyle={{ padding: '16px' }}
        >
            <ContentContainer>
                <Text size="lg" weight="bold" style={{ color: theme.colors.text }}>
                    {t('multiStep.education.heading')}
                </Text>
                <Text weight="medium" style={{ color: theme.colors.textSecondary }}>
                    {t('multiStep.education.intro')}
                </Text>
                <Text weight="medium" style={{ color: theme.colors.textSecondary }}>
                    <Trans
                        t={t}
                        i18nKey="multiStep.education.proTip"
                        components={{
                            label: <Text weight="bold" type="span" />,
                        }}
                    />
                </Text>
                <Text weight="medium" style={{ color: theme.colors.textSecondary }}>
                    {t('multiStep.education.lineage')}
                </Text>
                <StyledImage src={displayImage} preview={false} />
                <ItalicsText size="sm" style={{ color: theme.colors.textSecondary }}>
                    <Trans
                        t={t}
                        i18nKey="multiStep.education.cli"
                        components={{
                            anchor: (
                                <a href={EXTERNAL_SOURCE_REDIRECT_URL} target="_blank" rel="noreferrer">
                                    {t('multiStep.education.cliLinkText')}
                                </a>
                            ),
                        }}
                    />
                </ItalicsText>
            </ContentContainer>
        </Modal>
    );
}
