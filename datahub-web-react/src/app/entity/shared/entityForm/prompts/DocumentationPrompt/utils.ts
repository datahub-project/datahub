import { GenericEntityProperties } from '@app/entity/shared/types';
import { getFieldDescriptionDetails } from '@src/app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';
import { getAssetDescriptionDetails } from '@src/app/entityV2/shared/tabs/Documentation/utils';
import {
    EditableSchemaFieldInfo,
    FieldFormPromptAssociation,
    FormPromptAssociation,
    SchemaField,
} from '@src/types.generated';

export function getInitialDocumentationValues(
    entityData: GenericEntityProperties | null,
    field?: SchemaField,
    promptAssociation?: FormPromptAssociation | null,
    fieldPromptAssociation?: FieldFormPromptAssociation,
    editableFieldInfo?: EditableSchemaFieldInfo,
    enableInferredDescriptions?: boolean,
) {
    if (field) {
        const { displayedDescription } = getFieldDescriptionDetails({
            schemaFieldEntity: field.schemaFieldEntity,
            editableFieldInfo,
            defaultDescription: field.description,
            enableInferredDescriptions,
        });
        return fieldPromptAssociation?.response?.documentationResponse?.documentation || displayedDescription;
    }
    const { displayedDescription } = getAssetDescriptionDetails({
        entityProperties: entityData,
    });

    return promptAssociation?.response?.documentationResponse?.documentation || displayedDescription;
}
