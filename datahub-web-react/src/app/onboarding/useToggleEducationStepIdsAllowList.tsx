import { useContext, useEffect } from 'react';
import { EducationStepsContext } from '../../providers/EducationStepsContext';
import { useUpdateEducationStepsAllowList } from './useUpdateEducationStepsAllowList';

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
