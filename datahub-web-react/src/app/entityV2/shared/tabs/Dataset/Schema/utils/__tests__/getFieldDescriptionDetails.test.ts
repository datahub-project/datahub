import { getFieldDescriptionDetails } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/getFieldDescriptionDetails';

import { DocumentationAssociation, EditableSchemaFieldInfo, EntityType, SchemaFieldEntity } from '@types';

const mockActor = {
    urn: 'urn:li:corpuser:test',
    type: EntityType.CorpUser,
};

const createMockDocumentation = (
    documentation: string,
    time: number,
    sourceDetail: Array<{ key: string; value: string }> = [],
): DocumentationAssociation => ({
    documentation,
    attribution: {
        actor: mockActor,
        time,
        sourceDetail,
    },
});

const createMockSchemaFieldEntity = (documentations: DocumentationAssociation[]): SchemaFieldEntity => ({
    urn: 'urn:li:schemaField:test',
    type: EntityType.SchemaField,
    fieldPath: 'testField',
    documentation: {
        documentations,
    },
    parent: { urn: 'urn:li:dataset:test', type: EntityType.Dataset },
});

const createMockEditableFieldInfo = (description: string): EditableSchemaFieldInfo => ({
    fieldPath: 'testField',
    description,
});

describe('getFieldDescriptionDetails', () => {
    describe('basic cases', () => {
        it('returns empty string when all fields are undefined', () => {
            const result = getFieldDescriptionDetails({});

            expect(result.displayedDescription).toBe('');
            expect(result.isPropagated).toBe(false);
            expect(result.sourceDetail).toBeUndefined();
            expect(result.propagatedDescription).toBeUndefined();
            expect(result.attribution).toBeUndefined();
        });

        it('returns defaultDescription when only defaultDescription is provided', () => {
            const result = getFieldDescriptionDetails({
                defaultDescription: 'Default description',
            });

            expect(result.displayedDescription).toBe('Default description');
            expect(result.isPropagated).toBe(false);
        });

        it('handles null schemaFieldEntity', () => {
            const result = getFieldDescriptionDetails({
                schemaFieldEntity: null,
                defaultDescription: 'Default description',
            });

            expect(result.displayedDescription).toBe('Default description');
            expect(result.isPropagated).toBe(false);
        });
    });

    describe('description precedence', () => {
        it('prioritizes editableFieldInfo description over everything else', () => {
            const schemaFieldEntity = createMockSchemaFieldEntity([createMockDocumentation('Documentation text', 100)]);
            const editableFieldInfo = createMockEditableFieldInfo('Editable description');

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
                editableFieldInfo,
                defaultDescription: 'Default description',
            });

            expect(result.displayedDescription).toBe('Editable description');
            expect(result.isPropagated).toBe(false);
        });

        it('uses defaultDescription when editableFieldInfo is not provided', () => {
            const schemaFieldEntity = createMockSchemaFieldEntity([createMockDocumentation('Documentation text', 100)]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
                defaultDescription: 'Default description',
            });

            expect(result.displayedDescription).toBe('Default description');
            expect(result.isPropagated).toBe(false);
        });

        it('uses documentation when editableFieldInfo and defaultDescription are not provided', () => {
            const schemaFieldEntity = createMockSchemaFieldEntity([createMockDocumentation('Documentation text', 100)]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
            });

            expect(result.displayedDescription).toBe('Documentation text');
            expect(result.isPropagated).toBe(false);
        });

        it('prioritizes defaultDescription over documentation', () => {
            const schemaFieldEntity = createMockSchemaFieldEntity([createMockDocumentation('Documentation text', 100)]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
                defaultDescription: 'Default description',
            });

            expect(result.displayedDescription).toBe('Default description');
        });
    });

    describe('documentation sorting and filtering', () => {
        it('returns most recent documentation when multiple documentations exist', () => {
            const schemaFieldEntity = createMockSchemaFieldEntity([
                createMockDocumentation('Older doc', 100),
                createMockDocumentation('Newer doc', 200),
                createMockDocumentation('Oldest doc', 50),
            ]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
            });

            expect(result.displayedDescription).toBe('Newer doc');
        });

        it('handles documentations with missing time', () => {
            const docWithoutTime: DocumentationAssociation = {
                documentation: 'Doc without time',
                attribution: {
                    time: 0,
                    actor: mockActor,
                    sourceDetail: [],
                },
            };

            const schemaFieldEntity = createMockSchemaFieldEntity([
                docWithoutTime,
                createMockDocumentation('Doc with time', 100),
            ]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
            });

            expect(result.displayedDescription).toBe('Doc with time');
        });
    });

    describe('propagated documentation', () => {
        it('detects propagated documentation correctly', () => {
            const schemaFieldEntity = createMockSchemaFieldEntity([
                createMockDocumentation('Propagated doc', 100, [{ key: 'propagated', value: 'true' }]),
            ]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
            });

            expect(result.displayedDescription).toBe('Propagated doc');
            expect(result.isPropagated).toBe(true);
            expect(result.propagatedDescription).toBe('Propagated doc');
        });

        it('does not mark as propagated when value is false', () => {
            const schemaFieldEntity = createMockSchemaFieldEntity([
                createMockDocumentation('Non-propagated doc', 100, [{ key: 'propagated', value: 'false' }]),
            ]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
            });

            expect(result.isPropagated).toBe(false);
            expect(result.propagatedDescription).toBeUndefined();
        });

        it('does not mark as propagated when not using documentation aspect', () => {
            const schemaFieldEntity = createMockSchemaFieldEntity([
                createMockDocumentation('Propagated doc', 100, [{ key: 'propagated', value: 'true' }]),
            ]);
            const editableFieldInfo = createMockEditableFieldInfo('Editable description');

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
                editableFieldInfo,
            });

            expect(result.isPropagated).toBe(false);
            expect(result.propagatedDescription).toBeUndefined();
        });
    });

    describe('edge cases', () => {
        it('handles empty documentations array', () => {
            const schemaFieldEntity: SchemaFieldEntity = {
                urn: 'urn:li:schemaField:test',
                type: EntityType.SchemaField,
                fieldPath: 'testField',
                documentation: {
                    documentations: [],
                },
                parent: { urn: 'urn:li:dataset:test', type: EntityType.Dataset },
            };

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
                defaultDescription: 'Default description',
            });

            expect(result.displayedDescription).toBe('Default description');
            expect(result.isPropagated).toBe(false);
        });

        it('handles missing documentation field', () => {
            const schemaFieldEntity: SchemaFieldEntity = {
                urn: 'urn:li:schemaField:test',
                type: EntityType.SchemaField,
                fieldPath: 'testField',
                parent: { urn: 'urn:li:dataset:test', type: EntityType.Dataset },
            };

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
                defaultDescription: 'Default description',
            });

            expect(result.displayedDescription).toBe('Default description');
        });

        it('handles documentation without attribution', () => {
            const docWithoutAttribution: DocumentationAssociation = {
                documentation: 'Doc without attribution',
            };

            const schemaFieldEntity = createMockSchemaFieldEntity([docWithoutAttribution]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
            });

            expect(result.displayedDescription).toBe('Doc without attribution');
            expect(result.isPropagated).toBe(false);
            expect(result.attribution).toBeUndefined();
        });

        it('handles attribution without sourceDetail', () => {
            const docWithoutSourceDetail: DocumentationAssociation = {
                documentation: 'Doc without sourceDetail',
                attribution: {
                    actor: mockActor,
                    time: 100,
                },
            };

            const schemaFieldEntity = createMockSchemaFieldEntity([docWithoutSourceDetail]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
            });

            expect(result.displayedDescription).toBe('Doc without sourceDetail');
            expect(result.isPropagated).toBe(false);
            expect(result.sourceDetail).toBeUndefined();
        });

        it('handles empty description strings', () => {
            const editableFieldInfo = createMockEditableFieldInfo('');

            const result = getFieldDescriptionDetails({
                editableFieldInfo,
                defaultDescription: 'Default description',
            });

            expect(result.displayedDescription).toBe('');
        });

        it('handles null editableFieldInfo description', () => {
            const editableFieldInfo: EditableSchemaFieldInfo = {
                fieldPath: 'testField',
                description: null,
            };

            const result = getFieldDescriptionDetails({
                editableFieldInfo,
                defaultDescription: 'Default description',
            });

            expect(result.displayedDescription).toBe('Default description');
        });
    });

    describe('return object completeness', () => {
        it('returns all expected properties', () => {
            const schemaFieldEntity = createMockSchemaFieldEntity([
                createMockDocumentation('Test doc', 100, [{ key: 'propagated', value: 'true' }]),
            ]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
            });

            expect(result).toHaveProperty('displayedDescription');
            expect(result).toHaveProperty('isPropagated');
            expect(result).toHaveProperty('sourceDetail');
            expect(result).toHaveProperty('propagatedDescription');
            expect(result).toHaveProperty('attribution');
        });

        it('returns correct sourceDetail reference', () => {
            const sourceDetail = [{ key: 'test', value: 'value' }];
            const schemaFieldEntity = createMockSchemaFieldEntity([
                createMockDocumentation('Test doc', 100, sourceDetail),
            ]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
            });

            expect(result.sourceDetail).toBe(sourceDetail);
        });

        it('returns correct attribution reference', () => {
            const schemaFieldEntity = createMockSchemaFieldEntity([createMockDocumentation('Test doc', 100)]);

            const result = getFieldDescriptionDetails({
                schemaFieldEntity,
            });

            expect(result.attribution).toBe(schemaFieldEntity.documentation?.documentations?.[0]?.attribution);
        });
    });
});
