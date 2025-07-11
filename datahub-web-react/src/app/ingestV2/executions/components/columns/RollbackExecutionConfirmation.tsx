import React from 'react';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

interface Props {
    isOpen: boolean;
    onConfirm: () => void;
    onCancel: () => void;
}

export default function RollbackExecutionConfirmation({ isOpen, onConfirm, onCancel }: Props) {
    return (
        <ConfirmationModal
            isOpen={isOpen}
            modalTitle="Confirm Rollback"
            modalText="Are you sure you want to continue? 
            Rolling back this ingestion run will remove any new data ingested during the run. This may
            exclude data that was previously extracted, but did not change during this run."
            handleConfirm={onConfirm}
            handleClose={onCancel}
        />
    );
}
