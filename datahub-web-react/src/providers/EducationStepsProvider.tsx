import React, { useEffect, useState } from 'react';
import { useGetAuthenticatedUser } from '../app/useGetAuthenticatedUser';
import { getStepIds } from '../app/onboarding/utils';
import { useBatchGetStepStatesQuery } from '../graphql/step.generated';
import { EducationStepsContext } from './EducationStepsContext';
import { StepStateResult } from '../types.generated';
import { CURRENT_ONBOARDING_IDS } from '../app/onboarding/OnboardingConfig';

export function EducationStepsProvider({ children }: { children: React.ReactNode }) {
    const userUrn = useGetAuthenticatedUser()?.corpUser.urn;
    const stepIds = getStepIds(userUrn || '');
    const { data } = useBatchGetStepStatesQuery({ variables: { input: { ids: stepIds } } });
    const results = data?.batchGetStepStates.results;
    const [educationSteps, setEducationSteps] = useState<StepStateResult[] | null>(results || null);
    const [educationStepIdsAllowlist, setEducationStepIdsAllowlist] = useState<Set<string>>(
        new Set(CURRENT_ONBOARDING_IDS),
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
