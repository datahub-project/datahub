import { useCallback } from 'react';
import { useBatchUpdateStepStatesMutation } from '../../../graphql/step.generated';
import { LAST_VIEWED_ANNOUNCEMENT_TIME_STEP } from './utils';

export const useUpdateLastViewedAnnouncementTime = () => {
    const [updateStepState] = useBatchUpdateStepStatesMutation();
    const updateLastViewedAnnouncementTime = useCallback(
        async (userUrn: string) => {
            const currentTimestamp = new Date().getTime();
            const finalStepId = `${userUrn}-${LAST_VIEWED_ANNOUNCEMENT_TIME_STEP}`;
            await updateStepState({
                variables: {
                    input: {
                        states: [
                            {
                                id: finalStepId,
                                properties: [
                                    {
                                        key: LAST_VIEWED_ANNOUNCEMENT_TIME_STEP,
                                        value: currentTimestamp.toString(),
                                    },
                                ],
                            },
                        ],
                    },
                },
            });
        },
        [updateStepState],
    );

    return { updateLastViewedAnnouncementTime };
};
