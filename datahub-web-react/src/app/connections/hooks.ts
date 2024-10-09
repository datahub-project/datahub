/*
 * Hooks for managing connections
 */

import { message } from 'antd';
import { EntityType, DataHubConnectionDetailsType } from '@types';

import { useListSecretsQuery } from '@graphql/ingestion.generated';
import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import {
    useConnectionQuery,
    useUpsertConnectionMutation,
    useDeleteConnectionMutation,
} from '@graphql/connection.generated';

import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import { transformDotNotationToNested } from '@app/connections/utils';

/*
 * Hook to get connections for a given platform
 */
export const useGetConnections = ({ platformUrn }: { platformUrn: string }) => {
    const { data, loading, error, refetch } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.DatahubConnection],
                query: '*',
                start: 0,
                count: 50,
                orFilters: [{ and: [{ field: PLATFORM_FILTER_NAME, values: [platformUrn] }] }],
                searchFlags: { skipCache: true },
            },
        },
        skip: !platformUrn,
    });

    const connections = data?.searchAcrossEntities?.searchResults?.map((result) => result.entity);
    return { connections, loading, error, refetch };
};

/*
 * Hook to get a connection by id
 */
export const useGetConnection = ({ urn }: { urn?: string }) => {
    const { data, loading, error, refetch } = useConnectionQuery({
        variables: {
            urn: urn || '',
        },
        skip: !urn,
    });

    const connection = data?.connection;
    return { connection, loading, error, refetch };
};

/*
 * Hook to create a connection
 */
export const useCreateConnection = () => {
    const [upsertConnection, { data, loading, error }] = useUpsertConnectionMutation();

    const createConnection = ({ values, platformUrn }) => {
        message.loading({ content: 'Loading...', duration: 3 });

        // remove `name` from blob
        let blob = { ...values };
        delete blob.name;

        // unstructure dot-delimited fields
        blob = transformDotNotationToNested(JSON.stringify(blob));

        return upsertConnection({
            variables: {
                input: {
                    id: null,
                    platformUrn,
                    type: DataHubConnectionDetailsType.Json,
                    name: values.name,
                    json: {
                        blob: JSON.stringify(blob),
                    },
                },
            },
        });
    };

    return { createConnection, data, loading, error };
};

/*
 * Hook to update a connection
 */
export const useUpdateConnection = () => {
    const [upsertConnection, { data, loading, error }] = useUpsertConnectionMutation();
    const updateConnection = ({ id, values, platformUrn }) => {
        message.loading({ content: 'Loading...', duration: 3 });

        // remove `name` from blob
        let blob = { ...values };
        delete blob.name;

        // unstructure dot-delimited fields
        blob = transformDotNotationToNested(JSON.stringify(blob));

        // Extract the `id` from the `urn`
        const idOnly = id.split(':').pop();

        return upsertConnection({
            variables: {
                input: {
                    id: idOnly,
                    platformUrn,
                    type: DataHubConnectionDetailsType.Json,
                    name: values.name,
                    json: {
                        blob: JSON.stringify(blob),
                    },
                },
            },
        });
    };

    return { updateConnection, data, loading, error };
};

/*
 * Hook to delete a connection
 */
export const useDeleteConnection = () => {
    const [deleteConnection, { data, loading, error }] = useDeleteConnectionMutation();
    const removeConnection = (id: string) => {
        message.loading({ content: 'Loading...', duration: 3 });
        return deleteConnection({
            variables: {
                input: {
                    urn: id,
                    hardDelete: true,
                },
            },
        });
    };

    return { removeConnection, data, loading, error };
};

/*
 * Hook to get connection secrets
 */
export const useConnectionSecrets = () => {
    const { data, refetch: refetchSecrets } = useListSecretsQuery({
        variables: {
            input: {
                start: 0,
                count: 1000,
            },
        },
    });

    const secrets =
        data?.listSecrets?.secrets.sort((secretA, secretB) => secretA.name.localeCompare(secretB.name)) || [];

    return { secrets, refetchSecrets };
};
