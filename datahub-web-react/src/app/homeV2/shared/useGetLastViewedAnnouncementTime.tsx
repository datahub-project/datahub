import { useUserContext } from '@app/context/useUserContext';
import { LAST_VIEWED_ANNOUNCEMENT_TIME_STEP } from '@app/homeV2/shared/utils';

import { useBatchGetStepStatesQuery } from '@graphql/step.generated';

export const useGetLastViewedAnnouncementTime = () => {
    const { user } = useUserContext();
    const finalStepId = `${user?.urn}-${LAST_VIEWED_ANNOUNCEMENT_TIME_STEP}`;
    const { data, refetch } = useBatchGetStepStatesQuery({
        skip: !user?.urn,
        variables: { input: { ids: [finalStepId] } },
    });
    const lastViewedAnnouncementTimeProperty = data?.batchGetStepStates?.results?.[0]?.properties?.find(
        (property) => property.key === LAST_VIEWED_ANNOUNCEMENT_TIME_STEP,
    );
    return {
        time: (lastViewedAnnouncementTimeProperty?.value && Number(lastViewedAnnouncementTimeProperty?.value)) || null,
        refetch,
    };
};
