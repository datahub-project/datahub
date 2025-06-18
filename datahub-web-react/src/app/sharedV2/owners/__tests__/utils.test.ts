import {
    convertOwnerToPendingOwner,
    getOwnershipTypeDescriptionFromOwner,
    getOwnershipTypeNameFromOwner,
    isCorpUserOrCorpGroup,
    mapEntityToOwnerEntityType,
} from '@app/sharedV2/owners/utils';

import { CorpGroup, CorpUser, Entity, EntityType, Owner, OwnerEntityType, OwnershipType } from '@types';

const mockWarn = vi.spyOn(console, 'warn').mockImplementation(() => {});

describe('mapEntityToOwnerEntityType', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should map EntityType.CorpUser to OwnerEntityType.CorpUser', () => {
        const result = mapEntityToOwnerEntityType({ type: EntityType.CorpUser });
        expect(result).toBe(OwnerEntityType.CorpUser);
        expect(mockWarn).not.toHaveBeenCalled();
    });

    it('should map EntityType.CorpGroup to OwnerEntityType.CorpGroup', () => {
        const result = mapEntityToOwnerEntityType({ type: EntityType.CorpGroup });
        expect(result).toBe(OwnerEntityType.CorpGroup);
        expect(mockWarn).not.toHaveBeenCalled();
    });

    it('should default to OwnerEntityType.CorpUser when type is undefined', () => {
        const result = mapEntityToOwnerEntityType(undefined);
        expect(result).toBe(OwnerEntityType.CorpUser);
        expect(mockWarn).toHaveBeenCalledWith(
            `Entity with type undefined can't be mapped explicitly to OwnerEntityType. CorpUser will be used by default`,
        );
    });

    it('should warn and fallback to CorpUser for unknown entity types', () => {
        const fakeEntityType = 'UnknownType' as EntityType; // cast to simulate unknown value

        const result = mapEntityToOwnerEntityType({ type: fakeEntityType });
        expect(result).toBe(OwnerEntityType.CorpUser);
        expect(mockWarn).toHaveBeenCalledWith(
            `Entity with type UnknownType can't be mapped explicitly to OwnerEntityType. CorpUser will be used by default`,
        );
    });

    it('should handle empty object gracefully', () => {
        const result = mapEntityToOwnerEntityType({});
        expect(result).toBe(OwnerEntityType.CorpUser);
        expect(mockWarn).toHaveBeenCalledWith(
            `Entity with type undefined can't be mapped explicitly to OwnerEntityType. CorpUser will be used by default`,
        );
    });
});

describe('isCorpUserOrCorpGroup', () => {
    it('should return true for CorpUser', () => {
        const entity: CorpUser = {
            urn: 'user1',
            username: 'user1',
            type: EntityType.CorpUser,
        };

        expect(isCorpUserOrCorpGroup(entity)).toBe(true);
        expectTypeOf(entity).toMatchTypeOf<CorpUser | CorpGroup>();
    });

    it('should return true for CorpGroup', () => {
        const entity: CorpGroup = {
            urn: 'group1',
            name: 'group1',
            type: EntityType.CorpGroup,
        };

        expect(isCorpUserOrCorpGroup(entity)).toBe(true);
        expectTypeOf(entity).toMatchTypeOf<CorpUser | CorpGroup>();
    });

    it('should return false for non-CorpUser/CorpGroup entities', () => {
        const entity: Entity = {
            urn: 'someEntity',
            type: EntityType.Dataset,
        };

        expect(isCorpUserOrCorpGroup(entity)).toBe(false);
        expectTypeOf(entity).not.toMatchTypeOf<CorpUser | CorpGroup>();
    });

    it('should narrow type correctly in conditional blocks', () => {
        const entity: Entity = {
            urn: 'user1',
            type: EntityType.CorpUser,
        };

        if (isCorpUserOrCorpGroup(entity)) {
            // Should now be narrowed to CorpUser | CorpGroup
            expect(entity.type).toBe(EntityType.CorpUser);
            expectTypeOf(entity).toMatchTypeOf<CorpUser | CorpGroup>();
        } else {
            // This block won't run in this test, but TS should understand it's not CorpUser/Group here
            expectTypeOf(entity).not.toMatchTypeOf<CorpUser | CorpGroup>();
        }
    });
});

describe('getOwnershipTypeNameFromOwner', () => {
    it('should return info.name if available', () => {
        const owner: Partial<Owner> = {
            ownershipType: {
                info: {
                    name: 'Custom Type',
                },
                type: EntityType.CustomOwnershipType,
                urn: 'ownershipType',
            },
        };

        expect(getOwnershipTypeNameFromOwner(owner)).toBe('Custom Type');
    });

    it('should fallback to getNameFromType if info.name is missing', () => {
        const owner: Partial<Owner> = {
            type: OwnershipType.BusinessOwner,
        };

        expect(getOwnershipTypeNameFromOwner(owner)).toBe('Business Owner');
    });

    it('should return undefined if neither info.name nor type exists', () => {
        const owner: Partial<Owner> = {};

        expect(getOwnershipTypeNameFromOwner(owner)).toBeUndefined();
    });
});

describe('getOwnershipTypeDescriptionFromOwner', () => {
    it('should return info.description if available', () => {
        const owner: Partial<Owner> = {
            ownershipType: {
                info: {
                    name: '',
                    description: 'Custom Description',
                },
                type: EntityType.CustomOwnershipType,
                urn: 'ownershipType',
            },
        };

        expect(getOwnershipTypeDescriptionFromOwner(owner)).toBe('Custom Description');
    });

    it('should fallback to getDescriptionFromType if info.description is missing', () => {
        const owner: Partial<Owner> = {
            type: OwnershipType.DataSteward,
        };

        expect(getOwnershipTypeDescriptionFromOwner(owner)).toBe('Involved in governance of the asset(s).');
    });

    it('should return undefined if neither info.description nor type exists', () => {
        const owner: Partial<Owner> = {};

        expect(getOwnershipTypeDescriptionFromOwner(owner)).toBeUndefined();
    });
});

describe('convertOwnerToPendingOwner', () => {
    it('should convert a CorpUser owner to a PendingOwner', () => {
        const owner = {
            urn: 'urn:li:corpuser:testuser',
            type: EntityType.CorpUser,
        };
        const ownershipTypeUrn = 'urn:li:ownershipType:__system__Developer';

        const pendingOwner = convertOwnerToPendingOwner(owner, ownershipTypeUrn);

        expect(pendingOwner).toEqual({
            ownerUrn: 'urn:li:corpuser:testuser',
            ownerEntityType: OwnerEntityType.CorpUser,
            ownershipTypeUrn: 'urn:li:ownershipType:__system__Developer',
        });
    });

    it('should convert a CorpGroup owner to a PendingOwner', () => {
        const owner = {
            urn: 'urn:li:corpGroup:testgroup',
            type: EntityType.CorpGroup,
        };
        const ownershipTypeUrn = 'urn:li:ownershipType:__system__DATA_STEWARD';

        const pendingOwner = convertOwnerToPendingOwner(owner, ownershipTypeUrn);

        expect(pendingOwner).toEqual({
            ownerUrn: 'urn:li:corpGroup:testgroup',
            ownerEntityType: OwnerEntityType.CorpGroup,
            ownershipTypeUrn: 'urn:li:ownershipType:__system__DATA_STEWARD',
        });
    });

    it('should handle undefined ownershipTypeUrn', () => {
        const owner = {
            urn: 'urn:li:corpuser:testuser',
            type: EntityType.CorpUser,
        };

        const pendingOwner = convertOwnerToPendingOwner(owner, undefined);

        expect(pendingOwner).toEqual({
            ownerUrn: 'urn:li:corpuser:testuser',
            ownerEntityType: OwnerEntityType.CorpUser,
            ownershipTypeUrn: undefined,
        });
    });
});
