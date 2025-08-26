import { TestBuilderState } from '@app/tests/builder/types';
import { generateEditTestInput } from '@app/tests/card/testUtils';

import { EntityType, Test, TestMode } from '@types';

describe('testUtils', () => {
    describe('generateEditTestInput', () => {
        it('should handle TestBuilderState with all fields', () => {
            const input: TestBuilderState = {
                name: 'Test Name',
                category: 'Test Category',
                description: 'Test Description',
                definition: {
                    json: '{"key": "value"}',
                },
                status: {
                    mode: TestMode.Active,
                },
            };

            const result = generateEditTestInput(input);

            expect(result).toEqual({
                name: 'Test Name',
                category: 'Test Category',
                description: 'Test Description',
                definition: { json: '{"key": "value"}' },
                status: { mode: TestMode.Active },
            });
        });

        it('should handle TestBuilderState with missing optional fields', () => {
            const input: TestBuilderState = {
                name: 'Test Name',
                category: 'Test Category',
            };

            const result = generateEditTestInput(input);

            expect(result).toEqual({
                name: 'Test Name',
                category: 'Test Category',
                definition: { json: undefined },
                status: null,
            });
        });

        it('should handle TestBuilderState with empty strings', () => {
            const input: TestBuilderState = {
                name: '',
                category: '',
                description: '',
                definition: {
                    json: '',
                },
            };

            const result = generateEditTestInput(input);

            expect(result).toEqual({
                name: '',
                category: '',
                description: '',
                definition: { json: '' },
                status: null,
            });
        });

        it('should handle Test object input', () => {
            const input: Test = {
                urn: 'urn:li:test:123',
                type: EntityType.Test,
                results: { failingCount: 0, passingCount: 0 },
                name: 'Test Name',
                category: 'Test Category',
                description: 'Test Description',
                definition: {
                    json: '{"key": "value"}',
                },
                status: {
                    mode: TestMode.Active,
                },
            };

            const result = generateEditTestInput(input);

            expect(result).toEqual({
                name: 'Test Name',
                category: 'Test Category',
                description: 'Test Description',
                definition: { json: '{"key": "value"}' },
                status: { mode: TestMode.Active },
            });
        });

        it('should handle Test object with inactive status', () => {
            const input: Test = {
                urn: 'urn:li:test:123',
                type: EntityType.Test,
                results: { failingCount: 0, passingCount: 0 },
                name: 'Test Name',
                category: 'Test Category',
                description: 'Test Description',
                definition: {
                    json: '{"key": "value"}',
                },
                status: {
                    mode: TestMode.Inactive,
                },
            };

            const result = generateEditTestInput(input);

            expect(result).toEqual({
                name: 'Test Name',
                category: 'Test Category',
                description: 'Test Description',
                definition: { json: '{"key": "value"}' },
                status: { mode: TestMode.Inactive },
            });
        });

        it('should handle Test object with null status', () => {
            const input: Test = {
                urn: 'urn:li:test:123',
                type: EntityType.Test,
                results: { failingCount: 0, passingCount: 0 },
                name: 'Test Name',
                category: 'Test Category',
                description: 'Test Description',
                definition: {
                    json: '{"key": "value"}',
                },
                status: null,
            };

            const result = generateEditTestInput(input);

            expect(result).toEqual({
                name: 'Test Name',
                category: 'Test Category',
                description: 'Test Description',
                definition: { json: '{"key": "value"}' },
                status: null,
            });
        });
    });
});
