import { message } from 'antd';
import { useCallback } from 'react';

import analytics, { EventType } from '@app/analytics';
import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';

import { useUpdateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';
import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';
import { Entity, EntityType, OwnerEntityType, OwnershipTypeEntity, UpdateIngestionSourceInput } from '@types';

export function useUpdateIngestionSource() {
    const executeIngestionSource = useExecuteIngestionSource();
    const { data: ownershipTypesData } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
    });
    const [updateIngestionSource] = useUpdateIngestionSourceMutation();

    const [addOwners] = useBatchAddOwnersMutation();

    const ownershipTypes = ownershipTypesData?.listOwnershipTypes?.ownershipTypes || [];
    const defaultOwnerType: OwnershipTypeEntity | undefined = ownershipTypes.length > 0 ? ownershipTypes[0] : undefined;
    // const me = useUserContext();
    // const client = useApolloClient();

    const createSource = useCallback(
        async (sourceUrn: string, input: UpdateIngestionSourceInput, owners?: Entity[], shouldRun?: boolean) => {
            const ownerInputs = owners?.map((owner) => {
                return {
                    ownerUrn: owner.urn,
                    ownerEntityType:
                        owner.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser,
                    ownershipTypeUrn: defaultOwnerType?.urn,
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

                    // const updatedSource = {
                    //     config: {
                    //         ...input.config,
                    //         version: null,
                    //     },
                    //     name: input.name,
                    //     type: input.type,
                    //     schedule: input.schedule || null,
                    //     urn: sourceUrn,
                    //     ownership: {
                    //         owners: buildOwnerEntities(sourceUrn, owners, defaultOwnerType) || [],
                    //     },
                    // };
                    // updateListIngestionSourcesCache(client, updatedSource, queryInputs, false);

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
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [],
    );

    return createSource;
}
