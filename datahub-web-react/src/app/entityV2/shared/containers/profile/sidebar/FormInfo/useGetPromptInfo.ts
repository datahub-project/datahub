import { useMemo } from 'react';

import { useGetEntityWithSchema } from '@app/entity/shared/tabs/Dataset/Schema/useGetEntitySchema';
import { useEntityData } from '@app/entityV2/shared/EntityContext';
import {
    getAllPrompts,
    getEntityPromptsInfo,
    getFieldPromptsInfo,
    getPromptsForForm,
} from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/utils';

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
