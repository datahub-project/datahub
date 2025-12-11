/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
            Cancelling a running execution will NOT remove any data that has already been ingested. 
            You can use the DataHub CLI to rollback this ingestion run."
            handleConfirm={onConfirm}
            handleClose={onCancel}
        />
    );
}
