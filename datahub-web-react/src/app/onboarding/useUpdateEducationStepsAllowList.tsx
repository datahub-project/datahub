import { useContext, useEffect } from 'react';
import { EducationStepsContext } from '../../providers/EducationStepsContext';

// function use

export function useUpdateEducationStepsAllowList(condition?: boolean, id?: string) {
    const { educationStepIdsAllowlist, setEducationStepIdsAllowlist } = useContext(EducationStepsContext);

    function removeIdFromAllowList(newId: string) {
        const newStepdIdsAllowlist: Set<string> = new Set(educationStepIdsAllowlist);
        newStepdIdsAllowlist.delete(newId);
        setEducationStepIdsAllowlist(newStepdIdsAllowlist);
    }

    function addIdToAllowList(newId: string) {
        const newStepdIdsAllowlist: Set<string> = new Set(educationStepIdsAllowlist);
        newStepdIdsAllowlist.add(newId);
        setEducationStepIdsAllowlist(newStepdIdsAllowlist);
    }

    useEffect(() => {
        if (!id) return;

        const allowlistIncludesStepId = educationStepIdsAllowlist.has(id);

        if (condition && !allowlistIncludesStepId) {
            const newStepdIdsAllowlist: Set<string> = new Set(educationStepIdsAllowlist);
            newStepdIdsAllowlist.add(id);
            setEducationStepIdsAllowlist(newStepdIdsAllowlist);
        } else if (!condition && allowlistIncludesStepId) {
            const newStepdIdsAllowlist: Set<string> = new Set(educationStepIdsAllowlist);
            newStepdIdsAllowlist.delete(id);
            setEducationStepIdsAllowlist(newStepdIdsAllowlist);
        }
    }, [condition, id, educationStepIdsAllowlist, setEducationStepIdsAllowlist]);

    return { removeIdFromAllowList, addIdToAllowList };
}
