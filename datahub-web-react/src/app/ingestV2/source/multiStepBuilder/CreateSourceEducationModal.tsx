import { Modal, Text } from '@components';
import React from 'react';

import { INGESTION_SELECT_SOURCE_ID } from '@app/onboarding/config/IngestionOnboardingConfig';
import useHasSeenEducationStep from '@providers/hooks/useHasSeenEducationStep';
import useUpdateEducationStep from '@providers/hooks/useUpdateEducationStep';

export default function CreateSourceEducationModal() {
    const hasSeenStep = useHasSeenEducationStep(INGESTION_SELECT_SOURCE_ID);
    const { updateEducationStep } = useUpdateEducationStep();

    function handleClose() {
        updateEducationStep(INGESTION_SELECT_SOURCE_ID);
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
                    text: 'Get Started',
                    color: 'primary',
                },
            ]}
            title="About Metadata Collection"
            maskStyle={{ backgroundColor: 'rgba(0, 0, 0, 0.1)', backdropFilter: 'blur(2px)' }}
        >
            <Text color="gray" size="md">
                Choose the ingestion approach that works best for your security and infrastructure needs.
            </Text>
        </Modal>
    );
}
