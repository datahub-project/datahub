import { message } from 'antd';
import { useCallback } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { useExecuteIngestionSource } from '@app/ingestV2/source/hooks/useExecuteIngestionSource';
import { buildOwnerEntities } from '@app/ingestV2/source/utils';

import { useCreateIngestionSourceMutation } from '@graphql/ingestion.generated';
import { useBatchAddOwnersMutation } from '@graphql/mutations.generated';
import { useListOwnershipTypesQuery } from '@graphql/ownership.generated';
import {
    Entity,
    EntityType,
    IngestionSource,
    OwnerEntityType,
    OwnershipTypeEntity,
    UpdateIngestionSourceInput,
} from '@types';

const PLACEHOLDER_URN = 'placeholder-urn';

export function useCreateSource() {
    const executeIngestionSource = useExecuteIngestionSource();
    const [createIngestionSource] = useCreateIngestionSourceMutation();
    const { data: ownershipTypesData } = useListOwnershipTypesQuery({
        variables: {
            input: {},
        },
    });

    const [addOwners] = useBatchAddOwnersMutation();

    const ownershipTypes = ownershipTypesData?.listOwnershipTypes?.ownershipTypes || [];
    const defaultOwnerType: OwnershipTypeEntity | undefined = ownershipTypes.length > 0 ? ownershipTypes[0] : undefined;
    const me = useUserContext();

    const createSource = useCallback(
        async (input: UpdateIngestionSourceInput, owners?: Entity[], shouldRun?: boolean) => {
            const ownerInputs = owners?.map((owner) => {
                return {
                    ownerUrn: owner.urn,
                    ownerEntityType:
                        owner.type === EntityType.CorpGroup ? OwnerEntityType.CorpGroup : OwnerEntityType.CorpUser,
                    ownershipTypeUrn: defaultOwnerType?.urn,
                };
            });
            try {
                const result = await createIngestionSource({ variables: { input } });
                // message.loading({ content: 'Loading...', duration: 2 });
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
                        owners: buildOwnerEntities(newUrn, owners, defaultOwnerType),
                        lastModified: {
                            time: 0,
                        },
                        __typename: 'Ownership' as const,
                    },
                    __typename: 'IngestionSource' as const,
                };

                // addToListIngestionSourcesCache(client, newSource, {
                //     start: 0,
                //     count: DEFAULT_PAGE_SIZE,
                //     // query: undefined,
                //     filters: [getIngestionSourceSystemFilter(true)],
                //     // sort: undefined,
                // });

                if (ownersToAdd?.length) {
                    await addOwners({
                        variables: {
                            input: {
                                owners: ownersToAdd,
                                resources: [{ resourceUrn: newSource.urn }],
                            },
                        },
                    });
                }

                message.success({
                    content: `Successfully created ingestion source!`,
                    duration: 3,
                });

                if (result.data?.createIngestionSource) {
                    if (shouldRun) {
                        executeIngestionSource(result.data.createIngestionSource);
                    }
                }
            } catch (e: unknown) {
                message.destroy();
                if (e instanceof Error) {
                    message.error({
                        content: `Failed to create ingestion source!: \n ${e.message || ''}`,
                        duration: 3,
                    });
                }
            }
        },
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [],
    );

    return createSource;
}
