import { message } from 'antd';
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
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to cancel execution!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                })
                .finally(() => {
                    message.success({
                        content: `Successfully submitted cancellation request!`,
                        duration: 3,
                    });
                    // Refresh once a job was cancelled.
                    if (refetch) setTimeout(() => refetch(), REFETCH_TIMEOUT_MS);
                });
        },
        [refetch, cancelExecutionRequestMutation],
    );

    return onCancelExecutionRequest;
}
