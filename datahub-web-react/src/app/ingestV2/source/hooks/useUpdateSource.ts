import { message } from 'antd';
import { useCallback } from 'react';

import analytics, { EventType } from '@app/analytics';
import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';
import { useOwnershipTypes } from '@app/sharedV2/owners/useOwnershipTypes';

import { useUpdateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';
import { Entity, EntityType, OwnerEntityType, UpdateIngestionSourceInput } from '@types';

export function useUpdateIngestionSource() {
    const executeIngestionSource = useExecuteIngestionSource();

    const [updateIngestionSource] = useUpdateIngestionSourceMutation();

    const [addOwners] = useBatchAddOwnersMutation();

    const { defaultOwnershipType } = useOwnershipTypes();

    const createSource = useCallback(
        async (sourceUrn: string, input: UpdateIngestionSourceInput, owners?: Entity[], shouldRun?: boolean) => {
            const ownerInputs = owners?.map((owner) => {
                return {
                    ownerUrn: owner.urn,
                    ownerEntityType:
                        owner.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser,
                    ownershipTypeUrn: defaultOwnershipType?.urn,
                };
            });
            updateIngestionSource({ variables: { urn: sourceUrn as string, input } })
                .then(() => {
                    if (ownerInputs?.length) {
                        addOwners({
                            variables: {
                                input: {
                                    owners: ownerInputs,
                                    resources: [{ resourceUrn: sourceUrn }],
                                },
                            },
                        });
                    }

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
        [addOwners, defaultOwnershipType?.urn, executeIngestionSource, updateIngestionSource],
    );

    return createSource;
}
