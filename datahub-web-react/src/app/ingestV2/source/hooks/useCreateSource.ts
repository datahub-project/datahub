import { message } from 'antd';
import { useCallback } from 'react';

import analytics, { EventType } from '@app/analytics';
import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';
import { useAddOwners } from '@app/sharedV2/owners/useAddOwners';

import { useCreateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { Entity, UpdateIngestionSourceInput } from '@types';

export function useCreateSource() {
    const executeIngestionSource = useExecuteIngestionSource();
    const [createIngestionSource] = useCreateIngestionSourceMutation();

    const addOwners = useAddOwners();

    const createSource = useCallback(
        async (input: UpdateIngestionSourceInput, owners?: Entity[], shouldRun?: boolean) => {
            createIngestionSource({ variables: { input } })
                .then((result) => {
                    const newSourceUrn = result?.data?.createIngestionSource;

                    if (newSourceUrn) {
                        addOwners(owners, newSourceUrn);

                        analytics.event({
                            type: EventType.CreateIngestionSourceEvent,
                            sourceType: input.type,
                            sourceUrn: newSourceUrn,
                            interval: input.schedule?.interval,
                            numOwners: owners?.length,
                            outcome: shouldRun ? 'save_and_run' : 'save',
                        });
                        message.success({
                            content: `Successfully created ingestion source!`,
                            duration: 3,
                        });
                        if (shouldRun) executeIngestionSource(newSourceUrn);
                    } else {
                        message.destroy();
                        message.error({
                            content: 'Failed to create ingestion source!',
                            duration: 3,
                        });
                    }
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to create ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                });
        },
        [addOwners, createIngestionSource, executeIngestionSource],
    );

    return createSource;
}
