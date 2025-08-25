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
