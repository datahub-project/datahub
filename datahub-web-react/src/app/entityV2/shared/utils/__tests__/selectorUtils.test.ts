import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    buildEntityCache,
    createOwnerInputs,
    deduplicateEntities,
    entitiesToNestedSelectOptions,
    entitiesToSelectOptions,
    isEntityResolutionRequired,
    mergeSelectedIntoOptions,
    mergeSelectedNestedOptions,
} from '@app/entityV2/shared/utils/selectorUtils';

import { EntityType, OwnerEntityType, OwnershipType } from '@types';

// Mock entities for testing
const mockUser1 = {
    urn: 'urn:li:corpuser:user1',
    type: EntityType.CorpUser,
    properties: {
        displayName: 'User One',
        email: 'user1@example.com',
    },
};

const mockUser2 = {
    urn: 'urn:li:corpuser:user2',
    type: EntityType.CorpUser,
    properties: {
        displayName: 'User Two',
        email: 'user2@example.com',
    },
};

const mockGroup1 = {
    urn: 'urn:li:corpGroup:group1',
    type: EntityType.CorpGroup,
    properties: {
        displayName: 'Group One',
    },
};

const mockDomain1 = {
    urn: 'urn:li:domain:engineering',
    type: EntityType.Domain,
    properties: {
        name: 'Engineering',
        description: 'Engineering domain',
    },
    parentDomains: {
        domains: [{ urn: 'urn:li:domain:parent' }],
    },
};

// Mock entity registry
const mockEntityRegistry = {
    getDisplayName: vi.fn((type, entity) => {
        if (type === EntityType.CorpUser) {
            return entity.properties?.displayName || 'Unknown User';
        }
        if (type === EntityType.CorpGroup) {
            return entity.properties?.displayName || 'Unknown Group';
        }
        if (type === EntityType.Domain) {
            return entity.properties?.name || 'Unknown Domain';
        }
        return 'Unknown Entity';
    }),
};

describe('selectorUtils', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('buildEntityCache', () => {
        it('should build a cache map from entities array', () => {
            const entities = [mockUser1, mockUser2, mockGroup1];
            const cache = buildEntityCache(entities);

            expect(cache.size).toBe(3);
            expect(cache.get('urn:li:corpuser:user1')).toBe(mockUser1);
            expect(cache.get('urn:li:corpuser:user2')).toBe(mockUser2);
            expect(cache.get('urn:li:corpGroup:group1')).toBe(mockGroup1);
        });

        it('should handle empty entities array', () => {
            const cache = buildEntityCache([]);
            expect(cache.size).toBe(0);
        });

        it('should handle duplicate URNs by keeping the last one', () => {
            const duplicateUser = { ...mockUser1, properties: { displayName: 'Updated User' } };
            const entities = [mockUser1, duplicateUser];
            const cache = buildEntityCache(entities);

            expect(cache.size).toBe(1);
            expect(cache.get('urn:li:corpuser:user1')).toBe(duplicateUser);
        });
    });

    describe('isEntityResolutionRequired', () => {
        it('should return true when cache is missing some URNs', () => {
            const cache = new Map();
            cache.set('urn:li:corpuser:user1', mockUser1);

            const urns = ['urn:li:corpuser:user1', 'urn:li:corpuser:user2'];
            const result = isEntityResolutionRequired(urns, cache);

            expect(result).toBe(true);
        });

        it('should return false when cache has all URNs', () => {
            const cache = new Map();
            cache.set('urn:li:corpuser:user1', mockUser1);
            cache.set('urn:li:corpuser:user2', mockUser2);

            const urns = ['urn:li:corpuser:user1', 'urn:li:corpuser:user2'];
            const result = isEntityResolutionRequired(urns, cache);

            expect(result).toBe(false);
        });

        it('should return false for empty URNs array', () => {
            const cache = new Map();
            const result = isEntityResolutionRequired([], cache);

            expect(result).toBe(false);
        });
    });

    describe('deduplicateEntities', () => {
        it('should deduplicate entities from multiple sources', () => {
            const options = {
                placeholderEntities: [mockUser1, mockUser2],
                searchResults: [mockUser2, mockGroup1], // mockUser2 is duplicate
                selectedEntities: [mockUser1], // mockUser1 is duplicate
                existingEntityUrns: [],
            };

            const result = deduplicateEntities(options);

            expect(result).toHaveLength(3);
            expect(result.map((e) => e.urn)).toEqual([
                'urn:li:corpuser:user1',
                'urn:li:corpuser:user2',
                'urn:li:corpGroup:group1',
            ]);
        });

        it('should exclude existing entities but keep selected ones', () => {
            const options = {
                placeholderEntities: [mockUser1],
                searchResults: [mockUser2],
                selectedEntities: [mockUser1], // This should be included even if it's "existing"
                existingEntityUrns: ['urn:li:corpuser:user1', 'urn:li:corpuser:user2'],
            };

            const result = deduplicateEntities(options);

            expect(result).toHaveLength(1);
            expect(result[0].urn).toBe('urn:li:corpuser:user1'); // Selected entity included
        });

        it('should handle empty inputs', () => {
            const result = deduplicateEntities({});
            expect(result).toHaveLength(0);
        });

        it('should preserve order (first occurrence wins)', () => {
            const options = {
                placeholderEntities: [mockUser1],
                searchResults: [mockUser2, mockUser1], // mockUser1 appears later
                selectedEntities: [],
                existingEntityUrns: [],
            };

            const result = deduplicateEntities(options);

            expect(result).toHaveLength(2);
            expect(result[0].urn).toBe('urn:li:corpuser:user1'); // From placeholders (first)
            expect(result[1].urn).toBe('urn:li:corpuser:user2'); // From search results
        });
    });

    describe('entitiesToSelectOptions', () => {
        it('should convert entities to SelectOption format', () => {
            const entities = [mockUser1, mockGroup1];
            const result = entitiesToSelectOptions(entities, mockEntityRegistry as any);

            expect(result).toHaveLength(2);
            expect(result[0]).toEqual({
                value: 'urn:li:corpuser:user1',
                label: 'User One',
                description: 'user1@example.com',
            });
            expect(result[1]).toEqual({
                value: 'urn:li:corpGroup:group1',
                label: 'Group One',
                description: undefined,
            });
        });

        it('should handle empty entities array', () => {
            const result = entitiesToSelectOptions([], mockEntityRegistry as any);
            expect(result).toHaveLength(0);
        });

        it('should call entityRegistry.getDisplayName for labels', () => {
            const entities = [mockUser1];
            entitiesToSelectOptions(entities, mockEntityRegistry as any);

            expect(mockEntityRegistry.getDisplayName).toHaveBeenCalledWith(EntityType.CorpUser, mockUser1);
        });
    });

    describe('entitiesToNestedSelectOptions', () => {
        it('should convert cached entities to NestedSelectOption format', () => {
            const cache = new Map();
            cache.set('urn:li:domain:engineering', mockDomain1);

            const urns = ['urn:li:domain:engineering'];
            const result = entitiesToNestedSelectOptions(urns, cache, mockEntityRegistry as any);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                value: 'urn:li:domain:engineering',
                label: 'Engineering',
                id: 'urn:li:domain:engineering',
                parentId: 'urn:li:domain:parent',
                entity: mockDomain1,
            });
        });

        it('should create fallback options for uncached entities', () => {
            const cache = new Map(); // Empty cache
            const urns = ['urn:li:domain:engineering'];
            const result = entitiesToNestedSelectOptions(urns, cache, mockEntityRegistry as any);

            expect(result).toHaveLength(1);
            expect(result[0]).toEqual({
                value: 'urn:li:domain:engineering',
                label: 'engineering', // Extracted from URN
                id: 'urn:li:domain:engineering',
                parentId: undefined,
                entity: undefined,
            });
        });

        it('should handle malformed URNs gracefully', () => {
            const cache = new Map();
            const urns = ['invalid-urn'];
            const result = entitiesToNestedSelectOptions(urns, cache, mockEntityRegistry as any);

            expect(result).toHaveLength(1);
            expect(result[0].label).toBe('invalid-urn'); // Falls back to full URN
        });
    });

    describe('createOwnerInputs', () => {
        it('should create owner inputs with correct types', () => {
            const urns = ['urn:li:corpuser:user1', 'urn:li:corpGroup:group1'];

            const result = createOwnerInputs(urns);

            expect(result).toHaveLength(2);
            expect(result[0]).toEqual({
                ownerUrn: 'urn:li:corpuser:user1',
                ownerEntityType: OwnerEntityType.CorpUser,
                type: OwnershipType.BusinessOwner,
            });
            expect(result[1]).toEqual({
                ownerUrn: 'urn:li:corpGroup:group1',
                ownerEntityType: OwnerEntityType.CorpGroup,
                type: OwnershipType.BusinessOwner,
            });
        });

        it('should handle empty URNs array', () => {
            const result = createOwnerInputs([]);
            expect(result).toHaveLength(0);
        });

        it('should default to CorpUser for non-group URNs', () => {
            const urns = ['urn:li:unknown:entity'];
            const result = createOwnerInputs(urns);

            expect(result[0].ownerEntityType).toBe(OwnerEntityType.CorpUser);
        });
    });

    describe('mergeSelectedIntoOptions', () => {
        it('should merge selected options that are not in current options', () => {
            const options = [
                { value: 'option1', label: 'Option 1' },
                { value: 'option2', label: 'Option 2' },
            ];
            const selected = [
                { value: 'option2', label: 'Option 2' }, // Already in options
                { value: 'option3', label: 'Option 3' }, // Not in options
            ];

            const result = mergeSelectedIntoOptions(options, selected);

            expect(result).toHaveLength(3);
            expect(result.map((o) => o.value)).toEqual(['option1', 'option2', 'option3']);
        });

        it('should not duplicate options already present', () => {
            const options = [{ value: 'option1', label: 'Option 1' }];
            const selected = [
                { value: 'option1', label: 'Option 1' }, // Already in options
            ];

            const result = mergeSelectedIntoOptions(options, selected);

            expect(result).toHaveLength(1);
            expect(result[0].value).toBe('option1');
        });

        it('should handle empty arrays', () => {
            expect(mergeSelectedIntoOptions([], [])).toHaveLength(0);

            const options = [{ value: 'option1', label: 'Option 1' }];
            expect(mergeSelectedIntoOptions(options, [])).toEqual(options);
            expect(mergeSelectedIntoOptions([], options)).toEqual(options);
        });
    });

    describe('mergeSelectedNestedOptions', () => {
        it('should merge selected nested options', () => {
            const options = [{ value: 'domain1', label: 'Domain 1', id: 'domain1' }];
            const selected = [{ value: 'domain2', label: 'Domain 2', id: 'domain2' }];

            const result = mergeSelectedNestedOptions(options, selected);

            expect(result).toHaveLength(2);
            expect(result.map((o) => o.value)).toEqual(['domain1', 'domain2']);
        });

        it('should not duplicate nested options', () => {
            const option = { value: 'domain1', label: 'Domain 1', id: 'domain1' };
            const options = [option];
            const selected = [option];

            const result = mergeSelectedNestedOptions(options, selected);

            expect(result).toHaveLength(1);
            expect(result[0]).toBe(option);
        });
    });
});
