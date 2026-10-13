import { toast } from '@components';
import i18next from 'i18next';
import { useCallback } from 'react';

import { useRollbackIngestionMutation } from '@graphql/ingestion.generated';

const REFETCH_TIMEOUT_MS = 2000;

export default function useRollbackExecution(refetch: () => void) {
    const [rollbackIngestion] = useRollbackIngestionMutation();

    const rollbackExecution = useCallback(
        (runId: string) => {
            toast.loading(i18next.t('ingestion:executions.rollbackLoading'));

            rollbackIngestion({ variables: { input: { runId } } })
                .then(() => {
                    setTimeout(() => {
                        toast.destroy();
                        refetch();
                        toast.success(i18next.t('ingestion:executions.rollbackSuccess'));
                    }, REFETCH_TIMEOUT_MS);
                })
                .catch(() => {
                    toast.error(i18next.t('ingestion:executions.rollbackError'));
                });
        },
        [refetch, rollbackIngestion],
    );

    return rollbackExecution;
}
