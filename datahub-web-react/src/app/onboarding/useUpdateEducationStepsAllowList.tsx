import { useContext } from 'react';
import { EducationStepsContext } from '../../providers/EducationStepsContext';

// function use

export function useUpdateEducationStepsAllowList() {
    const { educationStepIdsAllowlist, setEducationStepIdsAllowlist } = useContext(EducationStepsContext);

    function removeIdFromAllowList(id: string) {
        const newStepdIdsAllowlist: Set<string> = new Set(educationStepIdsAllowlist);
        newStepdIdsAllowlist.delete(id);
        setEducationStepIdsAllowlist(newStepdIdsAllowlist);
    }

    function addIdToAllowList(id: string) {
        const newStepdIdsAllowlist: Set<string> = new Set(educationStepIdsAllowlist);
        newStepdIdsAllowlist.add(id);
        setEducationStepIdsAllowlist(newStepdIdsAllowlist);
    }

    return { removeIdFromAllowList, addIdToAllowList };
}
