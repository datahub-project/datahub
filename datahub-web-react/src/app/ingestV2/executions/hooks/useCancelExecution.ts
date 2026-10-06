import { message } from 'antd';
import i18next from 'i18next';
import { useCallback } from 'react';

import { useCancelIngestionExecutionRequestMutation } from '@graphql/ingestion.generated';

const REFETCH_TIMEOUT_MS = 2000;

export default function useCancelExecution(refetch?: () => void) {
    const [cancelExecutionRequestMutation] = useCancelIngestionExecutionRequestMutation();

    const onCancelExecutionRequest = useCallback(
        (executionUrn: string, ingestionSourceUrn: string) => {
            cancelExecutionRequestMutation({
                variables: {
                    input: {
                        ingestionSourceUrn,
                        executionRequestUrn: executionUrn,
                    },
                },
            })
                .then(() => {
                    message.success({
                        content: i18next.t('ingestion:executions.cancelSuccess'),
                        duration: 3,
                    });
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: i18next.t('ingestion:executions.cancelError', { errorMessage: e.message || '' }),
                        duration: 3,
                    });
                })
                .finally(() => {
                    // Refresh regardless of outcome to re-sync the executions list.
                    if (refetch) setTimeout(() => refetch(), REFETCH_TIMEOUT_MS);
                });
        },
        [refetch, cancelExecutionRequestMutation],
    );

    return onCancelExecutionRequest;
}
