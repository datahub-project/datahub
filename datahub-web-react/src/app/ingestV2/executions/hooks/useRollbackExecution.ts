import { toast } from '@components';
import i18next from 'i18next';
import { useCallback } from 'react';

import { useRollbackIngestionMutation } from '@graphql/ingestion.generated';

const REFETCH_TIMEOUT_MS = 2000;
const ROLLBACK_TOAST_KEY = 'ingestion-execution-rollback';

export default function useRollbackExecution(refetch: () => void) {
    const [rollbackIngestion] = useRollbackIngestionMutation();

    const rollbackExecution = useCallback(
        (runId: string) => {
            toast.loading(i18next.t('ingestion:executions.rollbackLoading'), { key: ROLLBACK_TOAST_KEY });

            rollbackIngestion({ variables: { input: { runId } } })
                .then(() => {
                    setTimeout(() => {
                        refetch();
                        toast.success(i18next.t('ingestion:executions.rollbackSuccess'), { key: ROLLBACK_TOAST_KEY });
                    }, REFETCH_TIMEOUT_MS);
                })
                .catch(() => {
                    toast.error(i18next.t('ingestion:executions.rollbackError'), { key: ROLLBACK_TOAST_KEY });
                });
        },
        [refetch, rollbackIngestion],
    );

    return rollbackExecution;
}
