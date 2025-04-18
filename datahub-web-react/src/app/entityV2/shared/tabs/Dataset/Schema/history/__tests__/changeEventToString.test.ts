import { ChangeCategoryType, ChangeEvent, ChangeOperationType } from '@src/types.generated';
import { getDocumentationString } from '../changeEventToString';

describe('getDocumentationString', () => {
    describe('Technical Schema Changes', () => {
        it('should handle adding a column with specified field path', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.TechnicalSchema,
                operation: ChangeOperationType.Add,
                parameters: [{ key: 'fieldPath', value: 'test.field.path' }],
                description: 'Original description',
            };

            const result = getDocumentationString(changeEvent);
            expect(result).toBe('Added column test.field.path.');
        });

        it('should handle removing a column', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.TechnicalSchema,
                operation: ChangeOperationType.Remove,
                parameters: [
                    {
                        key: 'fieldPath',
                        value: '[version=2.0].[type=struct].[type=array].[type=struct].addresses.[type=string].zip',
                    },
                ],
                description: 'Original description',
            };

            const result = getDocumentationString(changeEvent);
            expect(result).toBe('Removed column addresses.zip.');
        });

        it('should handle v2 field path column', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.TechnicalSchema,
                operation: ChangeOperationType.Remove,
                parameters: [{ key: 'fieldPath', value: 'test.field.path' }],
                description: 'Original description',
            };

            const result = getDocumentationString(changeEvent);
            expect(result).toBe('Removed column test.field.path.');
        });
    });

    describe('Documentation Changes', () => {
        it('should handle empty asset documentation', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Documentation,
                parameters: [{ key: 'description', value: '' }],
                description: 'Original description',
            };

            const result = getDocumentationString(changeEvent);
            expect(result).toBe('Asset documentation is empty.');
        });

        it('should handle setting asset documentation', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Documentation,
                parameters: [{ key: 'description', value: 'New asset description' }],
                description: 'Original description',
            };

            const result = getDocumentationString(changeEvent);
            expect(result).toBe('Set asset documentation to New asset description');
        });

        it('should handle setting field documentation', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Documentation,
                modifier: 'test.field',
                parameters: [{ key: 'description', value: 'New field description' }],
                description: 'Original description',
            };

            const result = getDocumentationString(changeEvent);
            expect(result).toBe('Set field documentation for test.field to New field description');
        });

        it('should handle setting v2 field documentation', () => {
            const changeEvent: ChangeEvent = {
                urn: 'urn:test',
                category: ChangeCategoryType.Documentation,
                modifier: '[version=2.0].[type=struct].[type=array].[type=struct].addresses.[type=string].zip',
                parameters: [{ key: 'description', value: 'New field description' }],
                description: 'Original description',
            };

            const result = getDocumentationString(changeEvent);
            expect(result).toBe('Set field documentation for addresses.zip to New field description');
        });
    });
});
