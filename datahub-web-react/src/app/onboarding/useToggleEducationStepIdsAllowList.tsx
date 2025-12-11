/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useContext, useEffect } from 'react';

import { useUpdateEducationStepsAllowList } from '@app/onboarding/useUpdateEducationStepsAllowList';
import { EducationStepsContext } from '@providers/EducationStepsContext';

export function useToggleEducationStepIdsAllowList(condition: boolean, id: string) {
    const { educationStepIdsAllowlist } = useContext(EducationStepsContext);
    const { addIdToAllowList, removeIdFromAllowList } = useUpdateEducationStepsAllowList();

    useEffect(() => {
        const allowlistIncludesStepId = educationStepIdsAllowlist.has(id);

        if (condition && !allowlistIncludesStepId) {
            addIdToAllowList(id);
        } else if (!condition && allowlistIncludesStepId) {
            removeIdFromAllowList(id);
        }
    }, [condition, id, addIdToAllowList, removeIdFromAllowList, educationStepIdsAllowlist]);
}
