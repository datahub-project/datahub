import { message } from 'antd';
import { useCallback } from 'react';

import analytics, { EventType } from '@app/analytics';
import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';
import { useUpdateOwners } from '@app/sharedV2/owners/useUpdateOwners';

import { useUpdateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { Entity, Owner, UpdateIngestionSourceInput } from '@types';

export function useUpdateIngestionSource() {
    const executeIngestionSource = useExecuteIngestionSource();

    const [updateIngestionSource] = useUpdateIngestionSourceMutation();

    const updateOwners = useUpdateOwners();

    const createSource = useCallback(
        async (
            sourceUrn: string,
            input: UpdateIngestionSourceInput,
            owners?: Entity[],
            existingOwners?: Owner[],
            shouldRun?: boolean,
        ) => {
            updateIngestionSource({ variables: { urn: sourceUrn as string, input } })
                .then(() => {
                    updateOwners(owners, existingOwners, sourceUrn);

                    analytics.event({
                        type: EventType.UpdateIngestionSourceEvent,
                        sourceType: input.type,
                        sourceUrn,
                        interval: input.schedule?.interval,
                        numOwners: owners?.length,
                        outcome: shouldRun ? 'save_and_run' : 'save',
                    });
                    message.success({
                        content: `Successfully updated ingestion source!`,
                        duration: 3,
                    });
                    if (shouldRun) executeIngestionSource(sourceUrn);
                })
                .catch((e) => {
                    message.destroy();
                    message.error({
                        content: `Failed to update ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                });
        },
        [updateOwners, executeIngestionSource, updateIngestionSource],
    );

    return createSource;
}
