import { describe, expect, it } from 'vitest';

import {
    entityDataWithNoAllowedOwners,
    entityDataWithNoAllowedTypes,
    entityDataWithNoOwners,
    mockEntityData,
    mockEntityData2,
    multipleRestrictedPrompt,
    multipleRestrictedWithTypesPrompt,
    multipleUnrestrictedPrompt,
    multipleUnrestrictedWithSingleTypePrompt,
    multipleUnrestrictedWithTypesPrompt,
    owner1,
    owner2,
    owner3,
    owner4,
    ownershipTypeUrn1,
    ownershipTypeUrn2,
    ownershipTypeUrn3,
    singleRestrictedPrompt,
    singleRestrictedWithTypesPrompt,
    singleUnrestrictedPrompt,
    singleUnrestrictedWithTypesPrompt,
    user1,
    user2,
    user3,
    user4,
} from '@app/entity/shared/entityForm/prompts/OwnershipPrompt/_tests_/mocks';
import {
    getDefaultOwnerEntities,
    getDefaultOwnershipTypeUrn,
} from '@app/entity/shared/entityForm/prompts/OwnershipPrompt/utils';

import { FormPrompt } from '@types';

describe('getDefaultOwnerEntities', () => {
    describe('when initialOwnershipTypeUrn is undefined', () => {
        it('should return empty array', () => {
            const result = getDefaultOwnerEntities(mockEntityData, multipleUnrestrictedPrompt, undefined);
            expect(result).toEqual([]);
        });
    });

    describe('when entityData is null', () => {
        it('should return empty array', () => {
            const result = getDefaultOwnerEntities(null, multipleUnrestrictedPrompt, ownershipTypeUrn1);
            expect(result).toEqual([]);
        });
    });

    describe('when entityData has no owners', () => {
        it('should return empty array', () => {
            const result = getDefaultOwnerEntities(
                entityDataWithNoOwners,
                multipleUnrestrictedPrompt,
                ownershipTypeUrn1,
            );
            expect(result).toEqual([]);
        });
    });

    describe('with MULTIPLE cardinality', () => {
        describe('unrestricted prompt', () => {
            it('should return all owners with matching ownership type', () => {
                const result = getDefaultOwnerEntities(mockEntityData2, multipleUnrestrictedPrompt, ownershipTypeUrn3);
                expect(result).toEqual([user3, user4]);
            });

            it('should return single owner when only one matches', () => {
                const result = getDefaultOwnerEntities(mockEntityData, multipleUnrestrictedPrompt, ownershipTypeUrn1);
                expect(result).toEqual([user1]);
            });

            it('should return empty array when no owners match ownership type', () => {
                const result = getDefaultOwnerEntities(
                    mockEntityData,
                    multipleUnrestrictedPrompt,
                    'urn:li:ownershipType:nonexistent',
                );
                expect(result).toEqual([]);
            });
        });

        describe('restricted prompt with allowed owners', () => {
            it('should return only allowed owners with matching ownership type', () => {
                const result = getDefaultOwnerEntities(mockEntityData, multipleRestrictedPrompt, ownershipTypeUrn3);
                expect(result).toEqual([user3]);
            });

            it('should return empty array when owner has matching type but is not in allowed list', () => {
                const result = getDefaultOwnerEntities(mockEntityData, multipleRestrictedPrompt, ownershipTypeUrn1);
                expect(result).toEqual([]);
            });

            it('should return multiple allowed owners with same ownership type', () => {
                // Create entity data where both user2 and user3 have the same ownership type
                const entityDataWithSameTypes = {
                    ownership: {
                        owners: [
                            { ...owner2, ownershipType: { urn: ownershipTypeUrn3, type: 'BUSINESS_OWNER' } },
                            { ...owner3, ownershipType: { urn: ownershipTypeUrn3, type: 'BUSINESS_OWNER' } },
                        ],
                    },
                };

                const result = getDefaultOwnerEntities(
                    entityDataWithSameTypes as any,
                    multipleRestrictedPrompt,
                    ownershipTypeUrn3,
                );
                expect(result).toEqual([user2, user3]);
            });
        });

        describe('with allowed ownership types', () => {
            it('should filter by ownership type correctly', () => {
                const result = getDefaultOwnerEntities(
                    mockEntityData,
                    multipleUnrestrictedWithTypesPrompt,
                    ownershipTypeUrn1,
                );
                expect(result).toEqual([user1]);
            });

            it('should return owners matching single allowed type', () => {
                const result = getDefaultOwnerEntities(
                    mockEntityData,
                    multipleUnrestrictedWithSingleTypePrompt,
                    ownershipTypeUrn2,
                );
                expect(result).toEqual([user2]);
            });
        });
    });

    describe('with SINGLE cardinality', () => {
        describe('unrestricted prompt', () => {
            it('should return first owner with matching ownership type', () => {
                const result = getDefaultOwnerEntities(mockEntityData2, singleUnrestrictedPrompt, ownershipTypeUrn3);
                expect(result).toEqual([user3]); // Should return only the first match
            });

            it('should return single owner when only one matches', () => {
                const result = getDefaultOwnerEntities(mockEntityData, singleUnrestrictedPrompt, ownershipTypeUrn1);
                expect(result).toEqual([user1]);
            });

            it('should return empty array when no owners match', () => {
                const result = getDefaultOwnerEntities(
                    mockEntityData,
                    singleUnrestrictedPrompt,
                    'urn:li:ownershipType:nonexistent',
                );
                expect(result).toEqual([]);
            });
        });

        describe('restricted prompt with allowed owners', () => {
            it('should return first allowed owner with matching ownership type', () => {
                const result = getDefaultOwnerEntities(mockEntityData, singleRestrictedPrompt, ownershipTypeUrn3);
                expect(result).toEqual([user3]);
            });

            it('should return empty array when no allowed owners match', () => {
                const result = getDefaultOwnerEntities(
                    entityDataWithNoAllowedOwners,
                    singleRestrictedPrompt,
                    ownershipTypeUrn1,
                );
                expect(result).toEqual([]);
            });
        });

        describe('with allowed ownership types', () => {
            it('should return first owner with allowed ownership type', () => {
                const result = getDefaultOwnerEntities(
                    mockEntityData,
                    singleUnrestrictedWithTypesPrompt,
                    ownershipTypeUrn1,
                );
                expect(result).toEqual([user1]);
            });

            it('should respect both allowed owners and ownership types', () => {
                const result = getDefaultOwnerEntities(
                    mockEntityData,
                    singleRestrictedWithTypesPrompt,
                    ownershipTypeUrn1,
                );
                expect(result).toEqual([]); // user1 has correct type but is not in allowed owners
            });
        });
    });

    describe('edge cases', () => {
        it('should handle prompt without ownershipParams', () => {
            const promptWithoutParams = {
                id: 'test',
                type: 'OWNERSHIP',
                required: true,
                formUrn: 'test',
                title: 'Test',
                ownershipParams: null,
            } as FormPrompt;

            const result = getDefaultOwnerEntities(mockEntityData, promptWithoutParams, ownershipTypeUrn1);
            expect(result).toEqual([user1]);
        });

        it('should handle empty allowedOwners array', () => {
            const promptWithEmptyAllowed = {
                ...multipleUnrestrictedPrompt,
                ownershipParams: {
                    ...multipleUnrestrictedPrompt.ownershipParams,
                    allowedOwners: [],
                },
            };

            const result = getDefaultOwnerEntities(mockEntityData, promptWithEmptyAllowed as any, ownershipTypeUrn1);
            expect(result).toEqual([user1]);
        });

        it('should handle owners without ownershipType', () => {
            const entityDataWithIncompleteOwner = {
                ownership: {
                    owners: [{ ...owner1, ownershipType: null }, owner2],
                },
            };

            const result = getDefaultOwnerEntities(
                entityDataWithIncompleteOwner as any,
                multipleUnrestrictedPrompt,
                ownershipTypeUrn2,
            );
            expect(result).toEqual([user2]);
        });
    });
});

describe('getDefaultOwnershipTypeUrn', () => {
    describe('when prompt has allowedOwnershipTypes', () => {
        it('should return first allowed ownership type', () => {
            const result = getDefaultOwnershipTypeUrn(mockEntityData, multipleUnrestrictedWithTypesPrompt);
            expect(result).toBe(ownershipTypeUrn1);
        });

        it('should return first allowed ownership type even when entity has different types', () => {
            const result = getDefaultOwnershipTypeUrn(
                entityDataWithNoAllowedTypes,
                multipleUnrestrictedWithTypesPrompt,
            );
            expect(result).toBe(ownershipTypeUrn1);
        });

        it('should handle empty allowedOwnershipTypes array', () => {
            const promptWithEmptyTypes = {
                ...multipleUnrestrictedWithTypesPrompt,
                ownershipParams: {
                    ...multipleUnrestrictedWithTypesPrompt.ownershipParams,
                    allowedOwnershipTypes: [],
                },
            };

            const result = getDefaultOwnershipTypeUrn(mockEntityData, promptWithEmptyTypes as any);
            expect(result).toBe(ownershipTypeUrn1); // Should fall back to entity's first ownership type
        });
    });

    describe('when prompt has allowedOwners but no allowedOwnershipTypes', () => {
        it('should return ownership type of first allowed owner that exists in entity', () => {
            const result = getDefaultOwnershipTypeUrn(mockEntityData, multipleRestrictedPrompt);
            expect(result).toBe(ownershipTypeUrn2); // user2 is first in allowed list and exists in entity
        });

        it('should return undefined when no allowed owners exist in entity', () => {
            const result = getDefaultOwnershipTypeUrn(entityDataWithNoAllowedOwners, multipleRestrictedPrompt);
            expect(result).toBeUndefined();
        });

        it('should handle allowed owners without ownership type', () => {
            const entityDataWithIncompleteOwner = {
                ownership: {
                    owners: [{ ...owner2, ownershipType: null }, owner3],
                },
            };

            const result = getDefaultOwnershipTypeUrn(entityDataWithIncompleteOwner as any, multipleRestrictedPrompt);
            expect(result).toBe(ownershipTypeUrn3); // Should skip user2 (no ownership type) and return user3's type
        });

        it('should handle empty allowedOwners array', () => {
            const promptWithEmptyOwners = {
                ...multipleRestrictedPrompt,
                ownershipParams: {
                    ...multipleRestrictedPrompt.ownershipParams,
                    allowedOwners: [],
                },
            };

            const result = getDefaultOwnershipTypeUrn(mockEntityData, promptWithEmptyOwners as any);
            expect(result).toBe(ownershipTypeUrn1); // Should fall back to entity's first ownership type
        });
    });

    describe('when prompt has no restrictions', () => {
        it('should return first distinct ownership type from entity', () => {
            const result = getDefaultOwnershipTypeUrn(mockEntityData, multipleUnrestrictedPrompt);
            expect(result).toBe(ownershipTypeUrn1);
        });

        it('should handle duplicate ownership types and return first unique one', () => {
            const result = getDefaultOwnershipTypeUrn(mockEntityData2, multipleUnrestrictedPrompt);
            expect(result).toBe(ownershipTypeUrn1); // Should deduplicate and return first
        });

        it('should return undefined when entity has no owners', () => {
            const result = getDefaultOwnershipTypeUrn(entityDataWithNoOwners, multipleUnrestrictedPrompt);
            expect(result).toBeUndefined();
        });
    });

    describe('edge cases', () => {
        it('should handle null entityData', () => {
            const result = getDefaultOwnershipTypeUrn(null, multipleUnrestrictedPrompt);
            expect(result).toBeUndefined();
        });

        it('should handle prompt without ownershipParams', () => {
            const promptWithoutParams = {
                id: 'test',
                type: 'OWNERSHIP',
                required: true,
                formUrn: 'test',
                title: 'Test',
                ownershipParams: null,
            } as FormPrompt;

            const result = getDefaultOwnershipTypeUrn(mockEntityData, promptWithoutParams);
            expect(result).toBe(ownershipTypeUrn1);
        });

        it('should handle entity with owners but no ownership types', () => {
            const entityDataWithoutTypes = {
                ownership: {
                    owners: [
                        { ...owner1, ownershipType: null },
                        { ...owner2, ownershipType: null },
                    ],
                },
            };

            const result = getDefaultOwnershipTypeUrn(entityDataWithoutTypes as any, multipleUnrestrictedPrompt);
            expect(result).toBeUndefined();
        });

        it('should handle entity with undefined ownership', () => {
            const entityDataWithoutOwnership = {} as any;

            const result = getDefaultOwnershipTypeUrn(entityDataWithoutOwnership, multipleUnrestrictedPrompt);
            expect(result).toBeUndefined();
        });

        it('should properly deduplicate ownership types including undefined values', () => {
            const entityDataWithMixedTypes = {
                ownership: {
                    owners: [
                        { ...owner1, ownershipType: { urn: ownershipTypeUrn1, type: 'DATA_STEWARD' } },
                        { ...owner2, ownershipType: null },
                        { ...owner3, ownershipType: { urn: ownershipTypeUrn1, type: 'DATA_STEWARD' } }, // Duplicate
                        { ...owner4, ownershipType: { urn: ownershipTypeUrn2, type: 'TECHNICAL_OWNER' } },
                    ],
                },
            };

            const result = getDefaultOwnershipTypeUrn(entityDataWithMixedTypes as any, multipleUnrestrictedPrompt);
            expect(result).toBe(ownershipTypeUrn1); // Should return first non-null unique type
        });
    });

    describe('priority order', () => {
        it('should prioritize allowedOwnershipTypes over allowedOwners', () => {
            // This prompt has both allowedOwnershipTypes and allowedOwners
            const result = getDefaultOwnershipTypeUrn(mockEntityData, multipleRestrictedWithTypesPrompt);
            expect(result).toBe(ownershipTypeUrn1); // Should use allowedOwnershipTypes[0], not derive from allowedOwners
        });

        it('should prioritize allowedOwners over entity ownership types', () => {
            const result = getDefaultOwnershipTypeUrn(mockEntityData, multipleRestrictedPrompt);
            expect(result).toBe(ownershipTypeUrn2); // Should use user2's type (first allowed owner), not user1's type (first entity owner)
        });

        it('should fall back to entity ownership types when no restrictions', () => {
            const result = getDefaultOwnershipTypeUrn(mockEntityData, multipleUnrestrictedPrompt);
            expect(result).toBe(ownershipTypeUrn1); // Should use first entity owner's type
        });
    });
});
