import { useContext, useEffect } from 'react';
import { EducationStepsContext } from '../../providers/EducationStepsContext';

export function useUpdateEducationStepIdsAllowlist(condition: boolean, id: string) {
    const { educationStepIdsAllowlist, setEducationStepIdsAllowlist } = useContext(EducationStepsContext);

    useEffect(() => {
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
}
