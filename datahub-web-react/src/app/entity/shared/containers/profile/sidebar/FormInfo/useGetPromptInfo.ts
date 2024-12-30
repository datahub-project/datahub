import { useMemo } from 'react';
import { useEntityData } from '../../../../EntityContext';
import { useGetEntityWithSchema } from '../../../../tabs/Dataset/Schema/useGetEntitySchema';
import { getAllPrompts, getEntityPromptsInfo, getFieldPromptsInfo, getPromptsForForm } from './utils';

export default function useGetPromptInfo(formUrn?: string) {
    const { entityData } = useEntityData();
    const { entityWithSchema } = useGetEntityWithSchema();
    const prompts = useMemo(
        () => (formUrn ? getPromptsForForm(formUrn, entityData) : getAllPrompts(entityData)),
        [formUrn, entityData],
    );

    const {
        entityPrompts,
        numRequiredEntityPromptsRemaining,
        numOptionalEntityPromptsRemaining,
        requiredEntityPrompts,
    } = getEntityPromptsInfo(prompts, entityData);
    const { fieldPrompts, numRequiredFieldPromptsRemaining, numOptionalFieldPromptsRemaining, requiredFieldPrompts } =
        getFieldPromptsInfo(prompts, entityData, entityWithSchema?.schemaMetadata?.fields?.length || 0, formUrn);
    // Multiply number of field prompts by number of schema fields for total number of schema field prompts
    const totalRequiredSchemaFieldPrompts =
        (entityWithSchema?.schemaMetadata?.fields?.length || 0) * requiredFieldPrompts.length;

    const numRequiredPromptsRemaining = numRequiredEntityPromptsRemaining + numRequiredFieldPromptsRemaining;
    const numOptionalPromptsRemaining = numOptionalEntityPromptsRemaining + numOptionalFieldPromptsRemaining;

    return {
        prompts,
        fieldPrompts,
        totalRequiredSchemaFieldPrompts,
        entityPrompts,
        numRequiredPromptsRemaining,
        numOptionalPromptsRemaining,
        requiredEntityPrompts,
    };
}
