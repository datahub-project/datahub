import { getOwnershipTypeDescription } from '@app/entityV2/shared/components/styled/ExpandedOwner/OwnerUtils';

import { EntityType, OwnershipTypeEntity } from '@types';

describe('getOwnershipTypeDescription', () => {
    const createMockOwnershipTypeEntity = (description?: string): OwnershipTypeEntity => ({
        urn: 'urn:li:ownershipType:test',
        type: EntityType.CustomOwnershipType,
        info: {
            name: 'Test Ownership Type',
            description,
            created: {
                time: 1234567890,
                actor: 'urn:li:corpuser:test',
            },
            lastModified: {
                time: 1234567890,
                actor: 'urn:li:corpuser:test',
            },
        },
        status: {
            removed: false,
        },
    });

    describe('with valid OwnershipTypeEntity', () => {
        it('should return description when ownershipType has info with description', () => {
            const ownershipType = createMockOwnershipTypeEntity('Technical owner responsible for data pipeline');

            const result = getOwnershipTypeDescription(ownershipType);

            expect(result).toBe('Technical owner responsible for data pipeline');
        });

        it('should return empty string description when description is empty', () => {
            const ownershipType = createMockOwnershipTypeEntity('');

            const result = getOwnershipTypeDescription(ownershipType);

            expect(result).toBe('');
        });

        it('should return undefined when description is undefined in info', () => {
            const ownershipType = createMockOwnershipTypeEntity(undefined);

            const result = getOwnershipTypeDescription(ownershipType);

            expect(result).toBeUndefined();
        });

        it('should handle long descriptions correctly', () => {
            const longDescription =
                'This is a very long description that describes the ownership type in great detail, including responsibilities, expectations, and scope of authority within the organization.';
            const ownershipType = createMockOwnershipTypeEntity(longDescription);

            const result = getOwnershipTypeDescription(ownershipType);

            expect(result).toBe(longDescription);
        });

        it('should handle descriptions with special characters', () => {
            const specialDescription = 'Description with special chars: @#$%^&*()[]{}|;\':",./<>?`~';
            const ownershipType = createMockOwnershipTypeEntity(specialDescription);

            const result = getOwnershipTypeDescription(ownershipType);

            expect(result).toBe(specialDescription);
        });

        it('should handle descriptions with newlines and whitespace', () => {
            const descriptionWithWhitespace = '  Description with\n  newlines\t  and\r\n  whitespace  ';
            const ownershipType = createMockOwnershipTypeEntity(descriptionWithWhitespace);

            const result = getOwnershipTypeDescription(ownershipType);

            expect(result).toBe(descriptionWithWhitespace);
        });
    });

    describe('with invalid or missing OwnershipTypeEntity', () => {
        it('should return undefined when ownershipType is undefined', () => {
            const result = getOwnershipTypeDescription(undefined);

            expect(result).toBeUndefined();
        });

        it('should return undefined when ownershipType is null', () => {
            const result = getOwnershipTypeDescription(null as any);

            expect(result).toBeUndefined();
        });

        it('should return undefined when ownershipType.info is undefined', () => {
            const ownershipType: OwnershipTypeEntity = {
                urn: 'urn:li:ownershipType:test',
                type: EntityType.CustomOwnershipType,
                status: {
                    removed: false,
                },
            } as any;

            const result = getOwnershipTypeDescription(ownershipType);

            expect(result).toBeUndefined();
        });

        it('should return undefined when ownershipType.info is null', () => {
            const ownershipType: OwnershipTypeEntity = {
                urn: 'urn:li:ownershipType:test',
                type: EntityType.CustomOwnershipType,
                info: null,
                status: {
                    removed: false,
                },
            } as any;

            const result = getOwnershipTypeDescription(ownershipType);

            expect(result).toBeUndefined();
        });

        it('should return undefined when ownershipType has info but no description property', () => {
            const ownershipType: OwnershipTypeEntity = {
                urn: 'urn:li:ownershipType:test',
                type: EntityType.CustomOwnershipType,
                info: {
                    name: 'Test Ownership Type',
                    created: {
                        time: 1234567890,
                        actor: 'urn:li:corpuser:test',
                    },
                    lastModified: {
                        time: 1234567890,
                        actor: 'urn:li:corpuser:test',
                    },
                } as any,
                status: {
                    removed: false,
                },
            };

            const result = getOwnershipTypeDescription(ownershipType);

            expect(result).toBeUndefined();
        });
    });

    describe('edge cases and boundary conditions', () => {
        it('should handle ownershipType with only required minimum properties', () => {
            const minimalOwnershipType: OwnershipTypeEntity = {
                urn: 'urn:li:ownershipType:minimal',
                type: EntityType.CustomOwnershipType,
                info: {
                    name: 'Minimal Type',
                    description: 'Minimal description',
                } as any,
            } as any;

            const result = getOwnershipTypeDescription(minimalOwnershipType);

            expect(result).toBe('Minimal description');
        });

        it('should not throw error when accessing description on malformed object', () => {
            const malformedOwnershipType = {
                info: {
                    description: 'Valid description in malformed object',
                },
            } as any;

            expect(() => {
                const result = getOwnershipTypeDescription(malformedOwnershipType);
                expect(result).toBe('Valid description in malformed object');
            }).not.toThrow();
        });

        it('should handle numeric description values', () => {
            const ownershipTypeWithNumericDescription = {
                info: {
                    description: 123 as any,
                },
            } as any;

            const result = getOwnershipTypeDescription(ownershipTypeWithNumericDescription);

            expect(result).toBe(123);
        });

        it('should handle boolean description values', () => {
            const ownershipTypeWithBooleanDescription = {
                info: {
                    description: true as any,
                },
            } as any;

            const result = getOwnershipTypeDescription(ownershipTypeWithBooleanDescription);

            expect(result).toBe(true);
        });

        it('should handle object description values', () => {
            const descriptionObject = { text: 'Object description' };
            const ownershipTypeWithObjectDescription = {
                info: {
                    description: descriptionObject as any,
                },
            } as any;

            const result = getOwnershipTypeDescription(ownershipTypeWithObjectDescription);

            expect(result).toBe(descriptionObject);
        });
    });

    describe('function behavior consistency', () => {
        it('should return the same result when called multiple times with same input', () => {
            const ownershipType = createMockOwnershipTypeEntity('Consistent description');

            const result1 = getOwnershipTypeDescription(ownershipType);
            const result2 = getOwnershipTypeDescription(ownershipType);
            const result3 = getOwnershipTypeDescription(ownershipType);

            expect(result1).toBe(result2);
            expect(result2).toBe(result3);
            expect(result1).toBe('Consistent description');
        });

        it('should not modify the input object', () => {
            const originalDescription = 'Original description';
            const ownershipType = createMockOwnershipTypeEntity(originalDescription);
            const originalOwnershipType = JSON.stringify(ownershipType);

            getOwnershipTypeDescription(ownershipType);

            expect(JSON.stringify(ownershipType)).toBe(originalOwnershipType);
            expect(ownershipType.info?.description).toBe(originalDescription);
        });

        it('should handle frozen objects without throwing errors', () => {
            const ownershipType = Object.freeze(createMockOwnershipTypeEntity('Frozen description'));

            expect(() => {
                const result = getOwnershipTypeDescription(ownershipType);
                expect(result).toBe('Frozen description');
            }).not.toThrow();
        });
    });

    describe('type safety and runtime behavior', () => {
        it('should handle objects that look like OwnershipTypeEntity but are not', () => {
            const fakeOwnershipType = {
                urn: 'fake-urn',
                type: 'fake-type',
                info: {
                    description: 'Fake but valid description',
                },
            } as any;

            const result = getOwnershipTypeDescription(fakeOwnershipType);

            expect(result).toBe('Fake but valid description');
        });

        it('should gracefully handle circular reference objects', () => {
            const circularObj: any = {
                info: {
                    description: 'Description with circular reference',
                },
            };
            circularObj.self = circularObj;

            const result = getOwnershipTypeDescription(circularObj);

            expect(result).toBe('Description with circular reference');
        });
    });
});
