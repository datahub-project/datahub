import { message } from 'antd';
import { useCallback } from 'react';

import analytics, { EventType } from '@app/analytics';
import { useUserContext } from '@app/context/useUserContext';
import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';
import { buildOwnerEntities } from '@app/ingestV2/source/utils';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';

import { useCreateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';
import { Entity, EntityType, IngestionSource, OwnerEntityType, UpdateIngestionSourceInput } from '@types';

const PLACEHOLDER_URN = 'placeholder-urn';

export function useCreateSource() {
    const executeIngestionSource = useExecuteIngestionSource();
    const [createIngestionSource] = useCreateIngestionSourceMutation();

    const [addOwners] = useBatchAddOwnersMutation();

    const { defaultOwnershipType } = useOwnershipTypes();

    const me = useUserContext();

    const createSource = useCallback(
        async (input: UpdateIngestionSourceInput, owners?: Entity[], shouldRun?: boolean) => {
            const ownerInputs = owners?.map((owner) => {
                return {
                    ownerUrn: owner.urn,
                    ownerEntityType:
                        owner.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser,
                    ownershipTypeUrn: defaultOwnershipType?.urn,
                };
            });

            createIngestionSource({ variables: { input } })
                .then((result) => {
                    message.loading({ content: 'Loading...', duration: 2 });
                    const ownersToAdd = ownerInputs?.filter((owner) => owner.ownerUrn !== me.urn);
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

                    if (ownersToAdd?.length) {
                        addOwners({
                            variables: {
                                input: {
                                    owners: ownersToAdd,
                                    resources: [{ resourceUrn: newSource.urn }],
                                },
                            },
                        });
                    }

                    analytics.event({
                        type: EventType.CreateIngestionSourceEvent,
                        sourceType: input.type,
                        sourceUrn: newSource.urn,
                        interval: input.schedule?.interval,
                        numOwners: ownersToAdd?.length,
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
        [addOwners, createIngestionSource, defaultOwnershipType, executeIngestionSource, me.urn],
    );

    return createSource;
}
