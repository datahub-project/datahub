import { message } from 'antd';
import { useCallback } from 'react';

import analytics, { EventType } from '@app/analytics';
import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';
import { buildOwnerEntities } from '@app/ingestV2/source/utils';
import { useAddOwners } from '@app/sharedV2/owners/useAddOwners';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';

import { useCreateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { Entity, IngestionSource, UpdateIngestionSourceInput } from '@types';

const PLACEHOLDER_URN = 'placeholder-urn';

export function useCreateSource() {
    const executeIngestionSource = useExecuteIngestionSource();
    const [createIngestionSource] = useCreateIngestionSourceMutation();

    const addOwners = useAddOwners();

    const { defaultOwnershipType } = useOwnershipTypes();

    const createSource = useCallback(
        async (input: UpdateIngestionSourceInput, owners?: Entity[], shouldRun?: boolean) => {
            createIngestionSource({ variables: { input } })
                .then((result) => {
                    message.loading({ content: 'Loading...', duration: 2 });
                    const newUrn = result?.data?.createIngestionSource || PLACEHOLDER_URN;

                    const newSource: IngestionSource = {
                        urn: newUrn,
                        name: input.name,
                        type: input.type,
                        config: { executorId: '', recipe: '', version: null, debugMode: null, extraArgs: null },
                        schedule: {
                            interval: input.schedule?.interval || '',
                            timezone: input.schedule?.timezone || null,
                        },
                        platform: null,
                        executions: null,
                        source: input.source || null,
                        ownership: {
                            owners: buildOwnerEntities(newUrn, owners, defaultOwnershipType),
                            lastModified: {
                                time: 0,
                            },
                            __typename: 'Ownership' as const,
                        },
                        __typename: 'IngestionSource' as const,
                    };

                    addOwners(owners, newUrn);

                    analytics.event({
                        type: EventType.CreateIngestionSourceEvent,
                        sourceType: input.type,
                        sourceUrn: newSource.urn,
                        interval: input.schedule?.interval,
                        numOwners: owners?.length,
                        outcome: shouldRun ? 'save_and_run' : 'save',
                    });
                    message.success({
                        content: `Successfully created ingestion source!`,
                        duration: 3,
                    });
                    if (result.data?.createIngestionSource && shouldRun) {
                        executeIngestionSource(result.data.createIngestionSource);
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
        [addOwners, createIngestionSource, defaultOwnershipType, executeIngestionSource],
    );

    return createSource;
}
