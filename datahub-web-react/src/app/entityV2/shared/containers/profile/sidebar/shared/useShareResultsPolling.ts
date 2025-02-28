import { useCallback, useEffect, useState } from 'react';
import moment from 'moment';
import { ShareResult, ShareResultState } from '../../../../../../../types.generated';
import { useEntityContext } from '../../../../../../entity/shared/EntityContext';
import { GenericEntityProperties } from '../../../../../../entity/shared/types';

function isShareResultRunning(result: ShareResult) {
    const isRunning = result.status === ShareResultState.Running;
    const isRecentlyAttempted = moment() < moment(result.statusLastUpdated).add(5, 'minute');
    return isRunning && isRecentlyAttempted;
}

export default function useShareResultsPolling() {
    const { entityData, refetch } = useEntityContext();
    const [isPolling, setIsPolling] = useState(false);

    const shouldPoll = useCallback((shareResults: ShareResult[], unshareResults: ShareResult[]) => {
        const isShareRunning = shareResults.some((result) => isShareResultRunning(result));
        const isUnshareRunning = unshareResults.some((result) => isShareResultRunning(result));
        return isShareRunning || isUnshareRunning;
    }, []);

    const refetchData = useCallback(
        (data: GenericEntityProperties) => {
            const shareResults = data.share?.lastShareResults || [];
            const unshareResults = data.share?.lastUnshareResults || [];
            if (shouldPoll(shareResults, unshareResults)) {
                refetch().then((result) => {
                    const genericEntityData = result.data[Object.keys(result.data)[0]];
                    setTimeout(() => refetchData(genericEntityData), 3000);
                });
            } else {
                setIsPolling(false);
            }
        },
        [shouldPoll, refetch],
    );

    useEffect(() => {
        const lastShareResults = entityData?.share?.lastShareResults || [];
        const lastUnshareResults = entityData?.share?.lastUnshareResults || [];
        if (entityData && shouldPoll(lastShareResults, lastUnshareResults) && !isPolling) {
            refetchData(entityData);
            setIsPolling(true);
        }
    }, [shouldPoll, isPolling, refetchData, entityData]);
}
