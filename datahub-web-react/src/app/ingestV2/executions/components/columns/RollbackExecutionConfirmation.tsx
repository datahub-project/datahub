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

export default function RollbackExecutionConfirmation({ isOpen, onConfirm, onCancel }: Props) {
    return (
        <ConfirmationModal
            isOpen={isOpen}
            modalTitle="Confirm Rollback"
            modalText={
                <>
                    Are you sure you want to continue? Rolling back this ingestion run will remove any new data ingested
                    during the run. This may exclude data that was previously extracted, but did not change during this
                    run.{' '}
                    <a
                        href="https://docs.datahub.com/docs/how/delete-metadata#rollback-ingestion-run"
                        target="_blank"
                        rel="noopener noreferrer"
                    >
                        Learn more
                    </a>
                </>
            }
            handleConfirm={onConfirm}
            handleClose={onCancel}
        />
    );
}
