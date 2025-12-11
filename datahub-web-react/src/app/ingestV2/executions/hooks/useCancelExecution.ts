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
