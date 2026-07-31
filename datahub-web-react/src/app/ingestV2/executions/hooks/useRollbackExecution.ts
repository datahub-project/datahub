import { message } from 'antd';
import i18next from 'i18next';
import { useCallback } from 'react';

import { useRollbackIngestionMutation } from '@graphql/ingestion.generated';

const REFETCH_TIMEOUT_MS = 2000;

export default function useRollbackExecution(refetch: () => void) {
    const [rollbackIngestion] = useRollbackIngestionMutation();

    const rollbackExecution = useCallback(
        (runId: string) => {
            message.loading(i18next.t('ingestion:executions.rollbackLoading'));

            rollbackIngestion({ variables: { input: { runId } } })
                .then(() => {
                    setTimeout(() => {
                        message.destroy();
                        refetch();
                        message.success(i18next.t('ingestion:executions.rollbackSuccess'));
                    }, REFETCH_TIMEOUT_MS);
                })
                .catch(() => {
                    message.error(i18next.t('ingestion:executions.rollbackError'));
                });
        },
        [refetch, rollbackIngestion],
    );

    return rollbackExecution;
}
