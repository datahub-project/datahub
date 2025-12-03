import { useCallback } from 'react';

import { useCreateIngestionExecutionRequestMutation } from '@graphql/ingestion.generated';

export function useExecuteIngestionSource() {
    const [createExecutionRequestMutation] = useCreateIngestionExecutionRequestMutation();

    const executeIngestionSource = useCallback(
        async (urn: string) => {
            return createExecutionRequestMutation({
                variables: {
                    input: { ingestionSourceUrn: urn },
                },
                refetchQueries: ['listIngestionExecutionRequests'],
            });
        },
        [createExecutionRequestMutation],
    );

    return executeIngestionSource;
}
