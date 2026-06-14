import { isAssignedToForm } from '@app/entity/shared/containers/profile/sidebar/FormInfo/useIsUserAssigned';

import { EntityType, FormAssociation, FormType, Owner, OwnershipType } from '@types';

const TECHNICAL_OWNER = 'urn:li:ownershipType:__system__technical_owner';
const DATA_STEWARD = 'urn:li:ownershipType:__system__data_steward';
const BUSINESS_OWNER = 'urn:li:ownershipType:__system__business_owner';

const USER_URN = 'urn:li:corpuser:user1';
const ENTITY_URN = 'urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)';

function ownerOf(userUrn: string, ownershipTypeUrn: string): Owner {
    return {
        associatedUrn: ENTITY_URN,
        owner: {
            __typename: 'CorpUser',
            urn: userUrn,
            type: EntityType.CorpUser,
            username: userUrn.split(':').pop() ?? '',
        },
        ownershipType: {
            __typename: 'OwnershipTypeEntity',
            urn: ownershipTypeUrn,
            type: EntityType.CustomOwnershipType,
        },
        type: OwnershipType.TechnicalOwner,
    } as Owner;
}

function formWithActors(actors: {
    owners: boolean;
    isAssignedToMe?: boolean;
    ownershipTypeUrns?: string[];
}): FormAssociation {
    return {
        associatedUrn: ENTITY_URN,
        form: {
            urn: 'urn:li:form:test',
            type: EntityType.Form,
            info: {
                name: 'test',
                type: FormType.Completion,
                actors: {
                    owners: actors.owners,
                    isAssignedToMe: actors.isAssignedToMe ?? false,
                    ownershipTypes: actors.ownershipTypeUrns
                        ? actors.ownershipTypeUrns.map(
                              (urn) =>
                                  ({
                                      __typename: 'OwnershipTypeEntity',
                                      urn,
                                      type: EntityType.CustomOwnershipType,
                                  }) as Owner['ownershipType'],
                          )
                        : undefined,
                },
                prompts: [],
            },
        },
        incompletePrompts: [],
        completedPrompts: [],
    } as unknown as FormAssociation;
}

describe('isAssignedToForm', () => {
    const ownersOfManyTypes: Owner[] = [
        ownerOf(USER_URN, TECHNICAL_OWNER),
        ownerOf('urn:li:corpuser:user2', DATA_STEWARD),
    ];

    it('returns true when the user is explicitly assigned via isAssignedToMe', () => {
        const form = formWithActors({ owners: false, isAssignedToMe: true });
        expect(isAssignedToForm(form, [], USER_URN)).toBe(true);
    });

    it('returns true when user is an owner and the form is assigned to owners (no type filter)', () => {
        const form = formWithActors({ owners: true });
        expect(isAssignedToForm(form, ownersOfManyTypes, USER_URN)).toBe(true);
    });

    it('returns false when user is not an owner and the form is owner-assigned', () => {
        const form = formWithActors({ owners: true });
        expect(isAssignedToForm(form, [], 'urn:li:corpuser:other')).toBe(false);
    });

    it('returns false when form is not owner-assigned and user is not explicitly assigned', () => {
        const form = formWithActors({ owners: false });
        expect(isAssignedToForm(form, ownersOfManyTypes, USER_URN)).toBe(false);
    });

    it('respects a single-ownership-type filter when set', () => {
        const matching = formWithActors({ owners: true, ownershipTypeUrns: [TECHNICAL_OWNER] });
        const nonMatching = formWithActors({ owners: true, ownershipTypeUrns: [BUSINESS_OWNER] });
        expect(isAssignedToForm(matching, ownersOfManyTypes, USER_URN)).toBe(true);
        expect(isAssignedToForm(nonMatching, ownersOfManyTypes, USER_URN)).toBe(false);
    });

    it('matches when at least one of multiple ownership types is satisfied', () => {
        const form = formWithActors({
            owners: true,
            ownershipTypeUrns: [TECHNICAL_OWNER, DATA_STEWARD],
        });
        expect(isAssignedToForm(form, ownersOfManyTypes, 'urn:li:corpuser:user2')).toBe(true);
    });

    it('treats an empty ownership-type list as "any owner"', () => {
        const form = formWithActors({ owners: true, ownershipTypeUrns: [] });
        expect(isAssignedToForm(form, ownersOfManyTypes, USER_URN)).toBe(true);
    });

    it('handles null/undefined owners gracefully', () => {
        const form = formWithActors({ owners: true });
        expect(isAssignedToForm(form, null, USER_URN)).toBe(false);
        expect(isAssignedToForm(form, undefined, USER_URN)).toBe(false);
    });
});
