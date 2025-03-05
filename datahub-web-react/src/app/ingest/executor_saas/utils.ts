import { EMBEDDED_EXECUTOR_POOL_NAME } from '@src/app/shared/constants';
import { RemoteExecutor, RemoteExecutorPool } from '@src/types.generated';

export const EXECUTOR_STALE_THRESHOLD_MS = 5 * 60 * 1000; // 5 minutes

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

export const checkIsExecutorStale = (
    executor: Pick<RemoteExecutor, 'reportedAt' | 'executorStopped' | 'executorExpired'>,
) => {
    return (
        Date.now() - executor.reportedAt > EXECUTOR_STALE_THRESHOLD_MS &&
        !executor.executorStopped &&
        !executor.executorExpired
    );
};
