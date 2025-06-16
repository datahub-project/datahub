import React from 'react';

import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';

interface Props {
    isOpen: boolean;
    onConfirm: () => void;
    onCancel: () => void;
}

export default function CancelExecutionConfirmation({ isOpen, onConfirm, onCancel }: Props) {
    return (
        <ConfirmationModal
            isOpen={isOpen}
            modalTitle="Confirm Cancel"
            modalText="Are you sure you want to continue? 
            Cancelling an running execution will NOT remove any data that has already been ingested. 
            You can use the DataHub CLI to rollback this ingestion run."
            handleConfirm={onConfirm}
            handleClose={onCancel}
        />
    );
}
