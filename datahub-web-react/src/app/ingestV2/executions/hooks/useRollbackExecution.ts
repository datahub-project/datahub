/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { message } from 'antd';
import { useCallback } from 'react';

import { useRollbackIngestionMutation } from '@graphql/ingestion.generated';

const REFETCH_TIMEOUT_MS = 2000;

export default function useRollbackExecution(refetch: () => void) {
    const [rollbackIngestion] = useRollbackIngestionMutation();

    const rollbackExecution = useCallback(
        (runId: string) => {
            message.loading('Requesting rollback...');

            rollbackIngestion({ variables: { input: { runId } } })
                .then(() => {
                    setTimeout(() => {
                        message.destroy();
                        refetch();
                        message.success('Successfully requested ingestion rollback');
                    }, REFETCH_TIMEOUT_MS);
                })
                .catch(() => {
                    message.error('Error requesting ingestion rollback');
                });
        },
        [refetch, rollbackIngestion],
    );

    return rollbackExecution;
}
