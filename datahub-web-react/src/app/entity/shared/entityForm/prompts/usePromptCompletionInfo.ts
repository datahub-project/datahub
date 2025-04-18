import { useMemo } from 'react';
import { EntityType, FormPrompt, SchemaField } from '../../../../../types.generated';
import { useUserContext } from '../../../../context/useUserContext';
import { useEntityRegistry } from '../../../../useEntityRegistry';
import { useEntityData } from '../../EntityContext';
import {
    findCompletedFieldPrompt,
    findPromptAssociation,
    getCompletedPrompts,
    getIncompletePrompts,
    isFieldPromptComplete,
    isPromptComplete,
} from '../../containers/profile/sidebar/FormInfo/utils';
import { getTimeFromNow } from '../../../../shared/time/timeUtils';

interface CompletionProps {
    prompt: FormPrompt;
    field?: SchemaField;
    optimisticCompletedTimestamp?: number | null;
}

export default function usePromptCompletionInfo({ prompt, field, optimisticCompletedTimestamp }: CompletionProps) {
    const { entityData } = useEntityData();
    const { user } = useUserContext();
    const entityRegistry = useEntityRegistry();
    const completedPrompts = getCompletedPrompts(entityData);
    const incompletePrompts = getIncompletePrompts(entityData);
    const promptAssociation = findPromptAssociation(prompt, completedPrompts.concat(incompletePrompts));
    const completedFieldPrompt = findCompletedFieldPrompt(field, promptAssociation);
    const isComplete =
        isPromptComplete(prompt, completedPrompts) ||
        isFieldPromptComplete(field, promptAssociation) ||
        !!optimisticCompletedTimestamp;

    const completedByName = useMemo(() => {
        let actor = completedFieldPrompt?.lastModified?.actor || promptAssociation?.lastModified?.actor;
        if (optimisticCompletedTimestamp) {
            actor = user;
        }
        return actor ? entityRegistry.getDisplayName(EntityType.CorpUser, actor) : '';
    }, [
        completedFieldPrompt?.lastModified?.actor,
        entityRegistry,
        optimisticCompletedTimestamp,
        promptAssociation?.lastModified?.actor,
        user,
    ]);

    const completedByTime = useMemo(() => {
        let completedTimestamp = completedFieldPrompt?.lastModified?.time || promptAssociation?.lastModified?.time;
        if (optimisticCompletedTimestamp) {
            completedTimestamp = optimisticCompletedTimestamp;
        }
        return completedTimestamp ? getTimeFromNow(completedTimestamp) : '';
    }, [completedFieldPrompt?.lastModified?.time, optimisticCompletedTimestamp, promptAssociation?.lastModified?.time]);

    return {
        completedByName,
        completedByTime,
        isComplete,
    };
}
