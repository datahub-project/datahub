import { GenericEntityProperties } from '@src/app/entity/shared/types';
import {
    EditableSchemaFieldInfo,
    EntityType,
    FieldFormPromptAssociation,
    FormPromptAssociation,
    SchemaField,
} from '@src/types.generated';

export const mockEntityData = {
    urn: 'urn:li:dataset:1',
    type: EntityType.Dataset,
    documentation: {
        documentations: [
            {
                documentation: 'Entity documentation',
            },
        ],
    },
} as GenericEntityProperties;

export const entityDataWithEditableDescription = {
    urn: 'urn:li:dataset:1',
    type: EntityType.Dataset,
    editableProperties: {
        description: 'Editable entity description',
    },
    documentation: {
        documentations: [
            {
                documentation: 'Entity documentation',
            },
        ],
    },
} as GenericEntityProperties;

export const entityDataWithNoDocumentation = {
    urn: 'urn:li:dataset:1',
    type: EntityType.Dataset,
} as GenericEntityProperties;

export const schemaField = {
    schemaFieldEntity: {
        fieldPath: 'field',
        type: EntityType.SchemaField,
        documentation: {
            documentations: [
                {
                    documentation: 'Schema field documentation',
                },
            ],
        },
    },
} as SchemaField;

export const noDocumentationField = {
    schemaFieldEntity: {
        fieldPath: 'field',
        type: EntityType.SchemaField,
    },
} as SchemaField;

export const fieldWithDescription = {
    description: 'Field description',
    schemaFieldEntity: {
        fieldPath: 'field',
        type: EntityType.SchemaField,
        documentation: {
            documentations: [
                {
                    documentation: 'Schema field documentation',
                },
            ],
        },
    },
} as SchemaField;

export const promptAssociation = {
    response: {
        documentationResponse: {
            documentation: 'Entity prompt response documentation',
        },
    },
} as FormPromptAssociation;

export const fieldAssociation = {
    response: {
        documentationResponse: {
            documentation: 'Field prompt response documentation',
        },
    },
} as FieldFormPromptAssociation;

export const editableFieldInfo = {
    fieldPath: 'field',
    description: 'Editable field description',
} as EditableSchemaFieldInfo;
