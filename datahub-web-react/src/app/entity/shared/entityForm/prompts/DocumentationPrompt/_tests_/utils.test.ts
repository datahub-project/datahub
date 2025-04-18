import {
    editableFieldInfo,
    entityDataWithEditableDescription,
    entityDataWithNoDocumentation,
    fieldAssociation,
    fieldWithDescription,
    mockEntityData,
    noDocumentationField,
    promptAssociation,
    schemaField,
} from '@app/entity/shared/entityForm/prompts/DocumentationPrompt/_tests_/mocks';
import { getInitialDocumentationValues } from '@app/entity/shared/entityForm/prompts/DocumentationPrompt/utils';

describe('get initial documentation values', () => {
    test('get default documentation of the entity', () => {
        const defaultDocumentation = getInitialDocumentationValues(mockEntityData);
        expect(defaultDocumentation).toEqual('Entity documentation');
    });

    test('get default documentation of the entity with editable description', () => {
        const defaultDocumentation = getInitialDocumentationValues(entityDataWithEditableDescription);
        expect(defaultDocumentation).toEqual('Editable entity description');
    });

    test('get default documentation of the entity when response in prompt association is present', () => {
        const defaultDocumentation = getInitialDocumentationValues(mockEntityData, undefined, promptAssociation);
        expect(defaultDocumentation).toEqual('Entity prompt response documentation');
    });

    test('get default documentation of the field with schema field entity documentation', () => {
        const defaultDocumentation = getInitialDocumentationValues(mockEntityData, schemaField);
        expect(defaultDocumentation).toEqual('Schema field documentation');
    });

    test('get default documentation of the field with field description', () => {
        const defaultDocumentation = getInitialDocumentationValues(mockEntityData, fieldWithDescription);
        expect(defaultDocumentation).toEqual('Field description');
    });

    test('get default documentation of the field when description is edited', () => {
        const defaultDocumentation = getInitialDocumentationValues(
            mockEntityData,
            schemaField,
            promptAssociation,
            undefined,
            editableFieldInfo,
        );
        expect(defaultDocumentation).toEqual('Editable field description');
    });

    test('get default documentation of the field when response in field association is present', () => {
        const defaultDocumentation = getInitialDocumentationValues(
            mockEntityData,
            schemaField,
            promptAssociation,
            fieldAssociation,
            editableFieldInfo,
        );
        expect(defaultDocumentation).toEqual('Field prompt response documentation');
    });

    test('get default documentation of the entity with no documentation present', () => {
        const defaultDocumentation = getInitialDocumentationValues(entityDataWithNoDocumentation);
        expect(defaultDocumentation).toEqual('');
    });

    test('get default documentation of the field with no documentation present', () => {
        const defaultDocumentation = getInitialDocumentationValues(mockEntityData, noDocumentationField);
        expect(defaultDocumentation).toEqual('');
    });
});
