/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useEffect, useState } from 'react';

import { useUserContext } from '@app/context/useUserContext';
import { getInitialAllowListIds, getStepIds } from '@app/onboarding/utils';
import { EducationStepsContext } from '@providers/EducationStepsContext';

import { useBatchGetStepStatesQuery } from '@graphql/step.generated';
import { StepStateResult } from '@types';

export function EducationStepsProvider({ children }: { children: React.ReactNode }) {
    const userUrn = useUserContext()?.user?.urn;
    const stepIds = getStepIds(userUrn || '');
    const { data } = useBatchGetStepStatesQuery({ skip: !userUrn, variables: { input: { ids: stepIds } } });
    const results = data?.batchGetStepStates?.results;
    const [educationSteps, setEducationSteps] = useState<StepStateResult[] | null>(results || null);
    const [educationStepIdsAllowlist, setEducationStepIdsAllowlist] = useState<Set<string>>(
        new Set(getInitialAllowListIds()),
    );

    useEffect(() => {
        if (results && (educationSteps === null || (educationSteps && results.length > educationSteps.length))) {
            setEducationSteps(results);
        }
    }, [results, educationSteps]);

    return (
        <EducationStepsContext.Provider
            value={{ educationSteps, setEducationSteps, educationStepIdsAllowlist, setEducationStepIdsAllowlist }}
        >
            {children}
        </EducationStepsContext.Provider>
    );
}
