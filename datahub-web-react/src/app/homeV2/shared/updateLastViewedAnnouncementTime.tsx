/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useCallback } from 'react';

import { LAST_VIEWED_ANNOUNCEMENT_TIME_STEP } from '@app/homeV2/shared/utils';

import { useBatchUpdateStepStatesMutation } from '@graphql/step.generated';

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
