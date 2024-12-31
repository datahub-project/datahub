import { EntityType, FormPromptType, FormType } from '../../../../types.generated';
import { GenericEntityProperties } from '../types';

const form1 = {
    urn: 'urn:li:form:1',
    type: EntityType.Form,
    info: {
        name: '',
        type: FormType.Verification,
        prompts: [
            {
                id: '1',
                type: FormPromptType.FieldsStructuredProperty,
                title: '',
                formUrn: 'urn:li:form:1',
                required: false,
            },
            {
                id: '2',
                type: FormPromptType.FieldsStructuredProperty,
                title: '',
                formUrn: 'urn:li:form:1',
                required: false,
            },
        ],
        actors: {
            owners: true,
            users: null,
            groups: null,
            isAssignedToMe: false,
        },
    },
};

const form2 = {
    urn: 'urn:li:form:2',
    type: EntityType.Form,
    info: {
        name: '',
        prompts: [
            {
                id: '3',
                type: FormPromptType.StructuredProperty,
                title: '',
                formUrn: 'urn:li:form:2',
                required: true,
            },
            {
                id: '4',
                type: FormPromptType.FieldsStructuredProperty,
                title: '',
                formUrn: 'urn:li:form:2',
                required: false,
            },
            {
                id: '5',
                type: FormPromptType.StructuredProperty,
                title: '',
                formUrn: 'urn:li:form:2',
                required: false,
            },
        ],
        type: FormType.Verification,
        actors: {
            owners: false,
            users: null,
            groups: null,
            isAssignedToMe: true,
        },
    },
};

export const mockEntityData = {
    schemaMetadata: { fields: [{ fieldPath: 'test' }] },
    forms: {
        verifications: [
            {
                form: form2,
                lastModified: {
                    actor: {
                        urn: 'urn:li:corpuser:test',
                    },
                    time: 100,
                },
            },
            {
                form: form2,
                lastModified: {
                    actor: {
                        urn: 'urn:li:corpuser:test',
                    },
                    time: 101,
                },
            },
        ],
        incompleteForms: [
            {
                completedPrompts: [
                    {
                        id: '1',
                        lastModified: { time: 123 },
                        fieldAssociations: {
                            completedFieldPrompts: [
                                { fieldPath: 'test3', lastModified: { time: 123 } },
                                { fieldPath: 'test4', lastModified: { time: 123 } },
                            ],
                        },
                    },
                ],
                incompletePrompts: [
                    {
                        id: '2',
                        lastModified: { time: 1234 },
                        fieldAssociations: {
                            completedFieldPrompts: [
                                { fieldPath: 'test1', lastModified: { time: 123 } },
                                { fieldPath: 'test2', lastModified: { time: 123 } },
                                { fieldPath: 'test3', lastModified: { time: 123 } },
                            ],
                        },
                    },
                ],
                associatedUrn: '',
                form: form1,
            },
        ],
        completedForms: [
            {
                completedPrompts: [{ id: '3', lastModified: { time: 1234 } }],
                incompletePrompts: [
                    { id: '4', lastModified: { time: 123 } },
                    { id: '5', lastModified: { time: 123 } },
                ],
                associatedUrn: '',
                form: form2,
            },
            {
                completedPrompts: [{ id: '6', lastModified: { time: 1234 } }],
                associatedUrn: '',
                form: {
                    urn: 'urn:li:form:3',
                    type: EntityType.Form,
                    info: {
                        name: '',
                        prompts: [
                            {
                                id: '6',
                                type: FormPromptType.StructuredProperty,
                                title: '',
                                formUrn: 'urn:li:form:3',
                                required: true,
                            },
                        ],
                        type: FormType.Completion,
                        actors: {
                            owners: true,
                            users: null,
                            groups: null,
                            isAssignedToMe: false,
                        },
                    },
                },
            },
        ],
    },
} as GenericEntityProperties;

export const mockEntityDataAllVerified = {
    ...mockEntityData,
    forms: {
        ...mockEntityData.forms,
        verifications: [
            {
                form: form2,
                lastModified: {
                    actor: {
                        urn: 'urn:li:corpuser:test',
                    },
                    time: 100,
                },
            },
            {
                form: form1,
                lastModified: {
                    actor: {
                        urn: 'urn:li:corpuser:test',
                    },
                    time: 101,
                },
            },
        ],
    },
} as GenericEntityProperties;

export const mockEntityDataWithFieldPrompts = {
    ...mockEntityData,
    forms: {
        ...mockEntityData.forms,
        incompleteForms: [
            {
                ...(mockEntityData as any).forms.incompleteForms[0],
                form: {
                    urn: 'urn:li:form:1',
                    type: EntityType.Form,
                    info: {
                        name: '',
                        prompts: [
                            {
                                id: '1',
                                type: FormPromptType.FieldsStructuredProperty,
                                title: '',
                                formUrn: 'urn:li:form:1',
                                required: false,
                            },
                        ],
                    },
                },
            },
        ],
    },
} as GenericEntityProperties;
