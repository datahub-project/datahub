import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
    ActorEntity,
    filterActors,
    findActorByUrn,
    getActorDisplayName,
    getActorEmail,
    getActorPictureLink,
    hasActorDisplayProperties,
    isActor,
    resolveActorsFromUrns,
} from '@app/entityV2/shared/utils/actorUtils';

import { CorpGroup, CorpUser, Entity, EntityType } from '@types';

// Mock entities for testing
const mockCorpUser: CorpUser = {
    urn: 'urn:li:corpuser:user1',
    type: EntityType.CorpUser,
    username: 'user1',
    properties: {
        displayName: 'John Doe',
        email: 'john.doe@example.com',
        fullName: 'John Doe',
        active: true,
        firstName: 'John',
        lastName: 'Doe',
    },
    editableProperties: {
        displayName: 'John Doe (Editable)',
        pictureLink: 'https://example.com/avatar1.jpg',
    },
} as CorpUser;

const mockCorpUser2: CorpUser = {
    urn: 'urn:li:corpuser:user2',
    type: EntityType.CorpUser,
    username: 'user2',
    properties: {
        displayName: 'Jane Smith',
        email: 'jane.smith@example.com',
        fullName: 'Jane Smith',
        active: true,
        firstName: 'Jane',
        lastName: 'Smith',
    },
} as CorpUser;

const mockCorpGroup: CorpGroup = {
    urn: 'urn:li:corpGroup:group1',
    type: EntityType.CorpGroup,
    name: 'engineering',
    properties: {
        displayName: 'Engineering Team',
    },
    editableProperties: {
        pictureLink: 'https://example.com/group-avatar.jpg',
    },
} as CorpGroup;

// Non-actor entities for negative tests
const mockDataset = {
    urn: 'urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)',
    type: EntityType.Dataset,
    name: 'SampleHiveDataset',
    properties: {
        name: 'Sample Dataset',
    },
} as Entity;

const mockDomain = {
    urn: 'urn:li:domain:engineering',
    type: EntityType.Domain,
    properties: {
        name: 'Engineering Domain',
    },
} as Entity;

describe('actorUtils', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('isActor', () => {
        it('should return true for CorpUser entities', () => {
            expect(isActor(mockCorpUser)).toBe(true);
        });

        it('should return true for CorpGroup entities', () => {
            expect(isActor(mockCorpGroup)).toBe(true);
        });

        it('should return false for non-actor entities', () => {
            expect(isActor(mockDataset)).toBe(false);
            expect(isActor(mockDomain)).toBe(false);
        });

        it('should return false for null or undefined', () => {
            expect(isActor(null)).toBe(false);
            expect(isActor(undefined)).toBe(false);
        });

        it('should return false for entities without type', () => {
            const entityWithoutType = { urn: 'test' } as Entity;
            expect(isActor(entityWithoutType)).toBe(false);
        });
    });

    describe('filterActors', () => {
        it('should filter out only actor entities', () => {
            const mixedEntities = [mockCorpUser, mockDataset, mockCorpGroup, mockDomain, null, undefined];

            const result = filterActors(mixedEntities);

            expect(result).toHaveLength(2);
            expect(result[0]).toBe(mockCorpUser);
            expect(result[1]).toBe(mockCorpGroup);
        });

        it('should return empty array when no actors present', () => {
            const nonActorEntities = [mockDataset, mockDomain, null, undefined];
            const result = filterActors(nonActorEntities);
            expect(result).toHaveLength(0);
        });

        it('should handle empty array', () => {
            const result = filterActors([]);
            expect(result).toHaveLength(0);
        });

        it('should preserve all actors when all entities are actors', () => {
            const actorEntities = [mockCorpUser, mockCorpGroup, mockCorpUser2];
            const result = filterActors(actorEntities);
            expect(result).toHaveLength(3);
            expect(result).toEqual([mockCorpUser, mockCorpGroup, mockCorpUser2]);
        });
    });

    describe('findActorByUrn', () => {
        it('should find actor by URN when it exists and is an actor', () => {
            const entities = [mockCorpUser, mockDataset, mockCorpGroup];

            const userResult = findActorByUrn(entities, mockCorpUser.urn);
            const groupResult = findActorByUrn(entities, mockCorpGroup.urn);

            expect(userResult).toBe(mockCorpUser);
            expect(groupResult).toBe(mockCorpGroup);
        });

        it('should return undefined when entity exists but is not an actor', () => {
            const entities = [mockCorpUser, mockDataset];
            const result = findActorByUrn(entities, mockDataset.urn);
            expect(result).toBeUndefined();
        });

        it('should return undefined when URN does not exist', () => {
            const entities = [mockCorpUser, mockCorpGroup];
            const result = findActorByUrn(entities, 'urn:li:corpuser:nonexistent');
            expect(result).toBeUndefined();
        });

        it('should handle empty entities array', () => {
            const result = findActorByUrn([], mockCorpUser.urn);
            expect(result).toBeUndefined();
        });
    });

    describe('resolveActorsFromUrns', () => {
        it('should resolve actors from multiple sources in priority order', () => {
            const urns = [
                mockCorpUser.urn, // Will be found in placeholderActors
                mockCorpUser2.urn, // Will be found in searchResults
                mockCorpGroup.urn, // Will be found in selectedActors
            ];

            const sources = {
                placeholderActors: [mockCorpUser],
                searchResults: [mockCorpUser2, mockDataset], // Mixed with non-actor
                selectedActors: [mockCorpGroup],
            };

            const result = resolveActorsFromUrns(urns, sources);

            expect(result).toHaveLength(3);
            expect(result[0]).toBe(mockCorpUser);
            expect(result[1]).toBe(mockCorpUser2);
            expect(result[2]).toBe(mockCorpGroup);
        });

        it('should prefer placeholderActors over other sources', () => {
            const urns = [mockCorpUser.urn];
            const differentUser = { ...mockCorpUser, properties: { displayName: 'Different Name' } } as CorpUser;

            const sources = {
                placeholderActors: [mockCorpUser],
                searchResults: [differentUser],
                selectedActors: [differentUser],
            };

            const result = resolveActorsFromUrns(urns, sources);

            expect(result).toHaveLength(1);
            expect(result[0]).toBe(mockCorpUser); // From placeholders, not the different one
        });

        it('should skip non-actor entities in search results', () => {
            const urns = [mockDataset.urn, mockCorpUser.urn];

            const sources = {
                searchResults: [mockDataset, mockCorpUser], // Mixed entities
            };

            const result = resolveActorsFromUrns(urns, sources);

            expect(result).toHaveLength(1); // Only the actor should be resolved
            expect(result[0]).toBe(mockCorpUser);
        });

        it('should handle empty URNs array', () => {
            const result = resolveActorsFromUrns([], {
                placeholderActors: [mockCorpUser],
            });
            expect(result).toHaveLength(0);
        });

        it('should handle empty sources', () => {
            const result = resolveActorsFromUrns([mockCorpUser.urn], {});
            expect(result).toHaveLength(0);
        });

        it('should handle URNs that cannot be resolved', () => {
            const urns = ['urn:li:corpuser:unknown', mockCorpUser.urn];

            const sources = {
                placeholderActors: [mockCorpUser],
            };

            const result = resolveActorsFromUrns(urns, sources);

            expect(result).toHaveLength(1); // Only the resolvable one
            expect(result[0]).toBe(mockCorpUser);
        });
    });

    describe('hasActorDisplayProperties', () => {
        it('should return true for valid actor entities', () => {
            expect(hasActorDisplayProperties(mockCorpUser)).toBe(true);
            expect(hasActorDisplayProperties(mockCorpGroup)).toBe(true);
        });

        it('should return false for non-actor entities', () => {
            expect(hasActorDisplayProperties(mockDataset)).toBe(false);
            expect(hasActorDisplayProperties(mockDomain)).toBe(false);
        });

        it('should return false for entities without URN', () => {
            const entityWithoutUrn = { type: EntityType.CorpUser } as Entity;
            expect(hasActorDisplayProperties(entityWithoutUrn)).toBe(false);
        });

        it('should return false for entities without type', () => {
            const entityWithoutType = { urn: 'test-urn' } as Entity;
            expect(hasActorDisplayProperties(entityWithoutType)).toBe(false);
        });
    });

    describe('getActorDisplayName', () => {
        it('should get display name from CorpUser properties', () => {
            expect(getActorDisplayName(mockCorpUser)).toBe('John Doe');
        });

        it('should fallback to fullName for CorpUser if displayName not available', () => {
            const userWithoutDisplayName = {
                ...mockCorpUser,
                properties: {
                    ...mockCorpUser.properties,
                    displayName: undefined,
                    fullName: 'John Doe Full',
                },
            } as CorpUser;

            expect(getActorDisplayName(userWithoutDisplayName)).toBe('John Doe Full');
        });

        it('should fallback to username for CorpUser if other names not available', () => {
            const userWithOnlyUsername = {
                ...mockCorpUser,
                properties: {
                    ...mockCorpUser.properties,
                    displayName: undefined,
                    fullName: undefined,
                },
            } as CorpUser;

            expect(getActorDisplayName(userWithOnlyUsername)).toBe('user1');
        });

        it('should get display name from CorpGroup properties', () => {
            expect(getActorDisplayName(mockCorpGroup)).toBe('Engineering Team');
        });

        it('should fallback to group name if displayName not available', () => {
            const groupWithoutDisplayName = {
                ...mockCorpGroup,
                properties: {
                    displayName: undefined,
                },
            } as CorpGroup;

            expect(getActorDisplayName(groupWithoutDisplayName)).toBe('engineering');
        });

        it('should return undefined for entities without any name properties', () => {
            const userWithoutNames = {
                ...mockCorpUser,
                username: undefined,
                properties: {
                    active: true,
                },
            } as unknown as CorpUser;

            expect(getActorDisplayName(userWithoutNames)).toBeUndefined();
        });
    });

    describe('getActorEmail', () => {
        it('should get email from CorpUser properties', () => {
            expect(getActorEmail(mockCorpUser)).toBe('john.doe@example.com');
        });

        it('should return undefined for CorpGroup entities', () => {
            expect(getActorEmail(mockCorpGroup)).toBeUndefined();
        });

        it('should return undefined for CorpUser without email', () => {
            const userWithoutEmail = {
                ...mockCorpUser,
                properties: {
                    ...mockCorpUser.properties,
                    email: undefined,
                },
            } as CorpUser;

            expect(getActorEmail(userWithoutEmail)).toBeUndefined();
        });

        it('should return undefined for CorpUser without properties', () => {
            const userWithoutProperties = {
                ...mockCorpUser,
                properties: undefined,
            } as CorpUser;

            expect(getActorEmail(userWithoutProperties)).toBeUndefined();
        });
    });

    describe('getActorPictureLink', () => {
        it('should get picture link from editableProperties for both users and groups', () => {
            expect(getActorPictureLink(mockCorpUser)).toBe('https://example.com/avatar1.jpg');
            expect(getActorPictureLink(mockCorpGroup)).toBe('https://example.com/group-avatar.jpg');
        });

        it('should return undefined when editableProperties is missing', () => {
            const userWithoutEditableProps = {
                ...mockCorpUser,
                editableProperties: undefined,
            } as CorpUser;

            expect(getActorPictureLink(userWithoutEditableProps)).toBeUndefined();
        });

        it('should return undefined when pictureLink is not set', () => {
            const userWithoutPicture = {
                ...mockCorpUser,
                editableProperties: {
                    pictureLink: undefined,
                },
            } as CorpUser;

            expect(getActorPictureLink(userWithoutPicture)).toBeUndefined();
        });

        it('should return undefined when editableProperties exists but is empty', () => {
            const userWithEmptyEditableProps = {
                ...mockCorpUser,
                editableProperties: {},
            } as CorpUser;

            expect(getActorPictureLink(userWithEmptyEditableProps)).toBeUndefined();
        });
    });

    // Integration test for typical usage patterns
    describe('integration scenarios', () => {
        it('should handle a complete actor resolution workflow', () => {
            // Simulate a typical component workflow
            const allUrns = [mockCorpUser.urn, mockCorpUser2.urn, mockCorpGroup.urn, 'urn:li:corpuser:missing'];

            // Mixed entity search results (including non-actors)
            const searchResults = [mockCorpUser2, mockDataset, mockDomain];
            const selectedActors = [mockCorpUser];
            const placeholderActors = [mockCorpGroup];

            // Step 1: Resolve actors from URNs
            const resolvedActors = resolveActorsFromUrns(allUrns, {
                placeholderActors,
                searchResults,
                selectedActors,
            });

            expect(resolvedActors).toHaveLength(3); // Missing one is skipped

            // Step 2: Filter mixed results to only actors
            const filteredSearch = filterActors(searchResults);
            expect(filteredSearch).toHaveLength(1); // Only mockCorpUser2

            // Step 3: Check display properties
            resolvedActors.forEach((actor) => {
                expect(hasActorDisplayProperties(actor)).toBe(true);
                expect(getActorDisplayName(actor)).toBeDefined();
            });

            // Step 4: Get specific properties
            const userActor = resolvedActors.find(isActor) as ActorEntity;
            if (userActor?.type === EntityType.CorpUser) {
                const email = getActorEmail(userActor);
                expect(typeof email === 'string' || email === undefined).toBe(true);
            }
        });
    });
});
