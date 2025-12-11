/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { StepStateResult } from '@types';

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
