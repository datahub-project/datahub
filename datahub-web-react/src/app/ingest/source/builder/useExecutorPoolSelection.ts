import { useEffect } from 'react';

import { useAppConfig } from '@app/useAppConfig';

import {
    useGetDefaultRemoteExecutorPoolQuery,
    useListRemoteExecutorPoolsQuery,
} from '@graphql/remote_executor.saas.generated';
import { RemoteExecutorPool } from '@types';

type Props = {
    searchQuery: string;
    currentExecutorId?: string;
    isEditing: boolean;
    onSetExecutorId: (id: string) => void;
};

export function useExecutorPoolSelection({ searchQuery, currentExecutorId, isEditing, onSetExecutorId }: Props) {
    const { config } = useAppConfig();
    const isExecutorPoolEnabled = config?.featureFlags?.displayExecutorPools;

    const { data, loading } = useListRemoteExecutorPoolsQuery({
        skip: !isExecutorPoolEnabled,
        variables: {
            query: searchQuery,
            count: 50,
            start: 0,
        },
        fetchPolicy: 'no-cache',
    });

    const { data: defaultPool } = useGetDefaultRemoteExecutorPoolQuery({ skip: !isExecutorPoolEnabled });

    const pools = (data?.listRemoteExecutorPools?.remoteExecutorPools || []) as RemoteExecutorPool[];
    const total = data?.listRemoteExecutorPools?.total || 0;
    const defaultPoolId = defaultPool?.defaultRemoteExecutorPool?.pool?.executorPoolId || pools[0]?.executorPoolId;

    useEffect(() => {
        // Only set default when:
        //  - Feature is enabled
        //  - Not editing mode
        //  - No current executor ID set
        //  - Default pool ID is available
        if (isExecutorPoolEnabled && !isEditing && !currentExecutorId && defaultPoolId) {
            onSetExecutorId(defaultPoolId);
        }
    }, [defaultPoolId, currentExecutorId, isEditing, onSetExecutorId, isExecutorPoolEnabled]);

    return {
        pools,
        loading,
        total,
        defaultPoolId,
    };
}
