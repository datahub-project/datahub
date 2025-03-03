import { EMBEDDED_EXECUTOR_POOL_NAME } from '@src/app/shared/constants';
import { RemoteExecutorPool } from '@src/types.generated';

export const getDisplayablePoolId = (
    pool: Pick<RemoteExecutorPool, 'executorPoolId' | 'isEmbedded'>,
    fallback = 'unknown',
) => {
    return pool.isEmbedded || pool.executorPoolId === EMBEDDED_EXECUTOR_POOL_NAME
        ? 'DataHub Embedded'
        : pool.executorPoolId || fallback;
};

export const checkIsPoolInDataHubCloud = (
    pool: Pick<RemoteExecutorPool, 'remoteExecutors' | 'isEmbedded' | 'executorPoolId'>,
) => {
    return (
        !!pool.remoteExecutors?.remoteExecutors?.find((executor) => executor.executorInternal) ||
        pool.isEmbedded ||
        pool.executorPoolId === EMBEDDED_EXECUTOR_POOL_NAME
    );
};
