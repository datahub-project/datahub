import React from 'react';
import { StepStateResult } from '../types.generated';

export const EducationStepsContext = React.createContext<{
    educationSteps: StepStateResult[] | null;
    setEducationSteps: React.Dispatch<React.SetStateAction<StepStateResult[] | null>>;
    educationStepIdsAllowlist: Set<string>;
    setEducationStepIdsAllowlist: React.Dispatch<React.SetStateAction<Set<string>>>;
}>({
    educationSteps: [],
    setEducationSteps: () => {},
    educationStepIdsAllowlist: new Set(),
    setEducationStepIdsAllowlist: () => {},
});
