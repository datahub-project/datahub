import { describe, expect, it } from 'vitest';

import {
    OwnedDomainLike,
    extractDomainOwners,
    extractOwnersFromDomains,
    filterDomainsByOwner,
    matchesDomainOwnerFilter,
} from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarFilters.utils';

import { CorpGroup, CorpUser, EntityType, Owner, Ownership, OwnershipType } from '@types';

// --- Fixtures ---------------------------------------------------------------
// Owners and the domain wrappers we pass into the utils. Cast through
// `unknown` so we only have to populate the fields the helpers actually read
// (this matches the pattern used by `documentTreeGrouping.test.ts`).
//
// `overrides` is typed as a loose record rather than `Partial<CorpUser>` /
// `Partial<CorpGroup>` because nested fields like `properties` are non-
// partial in the generated schema — supplying just `{ displayName }` would
// otherwise fail the structural check against `CorpUserProperties.active`.

type FixtureOverrides = Record<string, unknown>;

function makeUser(overrides: FixtureOverrides = {}): CorpUser {
    return {
        urn: 'urn:li:corpuser:jane',
        type: EntityType.CorpUser,
        username: 'jane',
        properties: { displayName: 'Jane Doe' },
        editableProperties: { displayName: undefined, pictureLink: 'https://example.com/jane.png' },
        ...overrides,
    } as unknown as CorpUser;
}

function makeGroup(overrides: FixtureOverrides = {}): CorpGroup {
    return {
        urn: 'urn:li:corpGroup:engineering',
        type: EntityType.CorpGroup,
        name: 'engineering',
        properties: { displayName: 'Engineering' },
        ...overrides,
    } as unknown as CorpGroup;
}

function makeOwner(owner: CorpUser | CorpGroup | null | undefined): Owner {
    return {
        owner,
        type: OwnershipType.None,
    } as unknown as Owner;
}

function makeDomain(owners: Array<Owner | null | undefined> | null): OwnedDomainLike {
    if (owners === null) {
        return { ownership: null } as OwnedDomainLike;
    }
    const ownership: Ownership = {
        owners,
    } as unknown as Ownership;
    return { ownership };
}

// --- extractDomainOwners ----------------------------------------------------

describe('extractDomainOwners', () => {
    it('returns an empty list when the domain has no ownership aspect', () => {
        expect(extractDomainOwners(makeDomain(null))).toEqual([]);
    });

    it('returns an empty list when the owners array is empty', () => {
        expect(extractDomainOwners(makeDomain([]))).toEqual([]);
    });

    it('projects a CorpUser owner into urn / displayName / type / pictureLink', () => {
        const user = makeUser();
        const result = extractDomainOwners(makeDomain([makeOwner(user)]));

        expect(result).toEqual([
            {
                urn: 'urn:li:corpuser:jane',
                displayName: 'Jane Doe',
                type: EntityType.CorpUser,
                pictureLink: 'https://example.com/jane.png',
            },
        ]);
    });

    it('projects a CorpGroup owner with a null pictureLink', () => {
        const group = makeGroup();
        const result = extractDomainOwners(makeDomain([makeOwner(group)]));

        expect(result).toEqual([
            {
                urn: 'urn:li:corpGroup:engineering',
                displayName: 'Engineering',
                type: EntityType.CorpGroup,
                pictureLink: null,
            },
        ]);
    });

    it('falls back through the user display-name chain', () => {
        // No displayName anywhere → falls back to fullName
        const userFullName = makeUser({
            properties: { fullName: 'Jane Q. Doe' } as CorpUser['properties'],
            editableProperties: undefined,
        });
        expect(extractDomainOwners(makeDomain([makeOwner(userFullName)]))[0].displayName).toBe('Jane Q. Doe');

        // No display fields at all → falls back to username
        const userUsername = makeUser({
            properties: undefined,
            editableProperties: undefined,
            info: undefined,
        });
        expect(extractDomainOwners(makeDomain([makeOwner(userUsername)]))[0].displayName).toBe('jane');

        // Nothing at all → falls back to URN
        const userUrnOnly = makeUser({
            properties: undefined,
            editableProperties: undefined,
            info: undefined,
            username: '',
        });
        expect(extractDomainOwners(makeDomain([makeOwner(userUrnOnly)]))[0].displayName).toBe('urn:li:corpuser:jane');
    });

    it('falls back through the group display-name chain (displayName → name → urn)', () => {
        const groupName = makeGroup({ properties: undefined });
        expect(extractDomainOwners(makeDomain([makeOwner(groupName)]))[0].displayName).toBe('engineering');

        const groupUrnOnly = makeGroup({ properties: undefined, name: '' });
        expect(extractDomainOwners(makeDomain([makeOwner(groupUrnOnly)]))[0].displayName).toBe(
            'urn:li:corpGroup:engineering',
        );
    });

    it('skips malformed owner rows (null / missing owner payload)', () => {
        const user = makeUser();
        const result = extractDomainOwners(
            makeDomain([null, undefined, makeOwner(null), makeOwner(undefined), makeOwner(user)]),
        );

        expect(result).toHaveLength(1);
        expect(result[0].urn).toBe(user.urn);
    });

    it('ignores owner entities of unsupported types', () => {
        // Some made-up "Role" owner type the projection does not understand
        const role = { urn: 'urn:li:role:admin', type: EntityType.Role } as unknown as CorpUser;
        const user = makeUser();
        const result = extractDomainOwners(makeDomain([makeOwner(role), makeOwner(user)]));

        expect(result).toHaveLength(1);
        expect(result[0].type).toBe(EntityType.CorpUser);
    });
});

// --- extractOwnersFromDomains -----------------------------------------------

describe('extractOwnersFromDomains', () => {
    it('returns an empty list when given an empty array', () => {
        expect(extractOwnersFromDomains([])).toEqual([]);
    });

    it('flattens owners across every domain in input order (does not dedupe)', () => {
        const jane = makeUser({ urn: 'urn:li:corpuser:jane' });
        const john = makeUser({ urn: 'urn:li:corpuser:john', properties: { displayName: 'John' } });
        const eng = makeGroup();

        const domains: OwnedDomainLike[] = [
            makeDomain([makeOwner(jane), makeOwner(eng)]),
            makeDomain([makeOwner(john), makeOwner(jane)]),
        ];

        const result = extractOwnersFromDomains(domains);

        // Dedup is the context's job; the util just flattens in order.
        expect(result.map((o) => o.urn)).toEqual([
            'urn:li:corpuser:jane',
            'urn:li:corpGroup:engineering',
            'urn:li:corpuser:john',
            'urn:li:corpuser:jane',
        ]);
    });

    it('skips domains with no ownership aspect', () => {
        const jane = makeUser();
        const result = extractOwnersFromDomains([makeDomain(null), makeDomain([makeOwner(jane)]), makeDomain([])]);
        expect(result).toHaveLength(1);
        expect(result[0].urn).toBe(jane.urn);
    });
});

// --- matchesDomainOwnerFilter -----------------------------------------------

describe('matchesDomainOwnerFilter', () => {
    const jane = makeUser({ urn: 'urn:li:corpuser:jane' });
    const john = makeUser({ urn: 'urn:li:corpuser:john' });
    const domain = makeDomain([makeOwner(jane), makeOwner(john)]);

    it('passes every domain when the selection is empty', () => {
        expect(matchesDomainOwnerFilter(domain, [])).toBe(true);
        expect(matchesDomainOwnerFilter(makeDomain([]), [])).toBe(true);
    });

    it('passes every domain when the selection is null', () => {
        expect(matchesDomainOwnerFilter(domain, null)).toBe(true);
    });

    it('matches when any one of the selected URNs is in the owner list (OR semantics)', () => {
        expect(matchesDomainOwnerFilter(domain, ['urn:li:corpuser:jane'])).toBe(true);
        expect(matchesDomainOwnerFilter(domain, ['urn:li:corpuser:not-an-owner', 'urn:li:corpuser:john'])).toBe(true);
    });

    it('rejects when no selected URN appears in the owner list', () => {
        expect(matchesDomainOwnerFilter(domain, ['urn:li:corpuser:not-an-owner'])).toBe(false);
    });

    it('rejects when the domain has no ownership and a filter is active', () => {
        expect(matchesDomainOwnerFilter(makeDomain(null), ['urn:li:corpuser:jane'])).toBe(false);
        expect(matchesDomainOwnerFilter(makeDomain([]), ['urn:li:corpuser:jane'])).toBe(false);
    });
});

// --- filterDomainsByOwner ---------------------------------------------------

describe('filterDomainsByOwner', () => {
    const jane = makeUser({ urn: 'urn:li:corpuser:jane' });
    const john = makeUser({ urn: 'urn:li:corpuser:john' });

    it('returns a shallow copy of the input when the filter is empty', () => {
        const domains = [makeDomain([makeOwner(jane)]), makeDomain([makeOwner(john)])];

        expect(filterDomainsByOwner(domains, [])).toEqual(domains);
        expect(filterDomainsByOwner(domains, null)).toEqual(domains);
        // Crucially, returns a NEW array — callers can mutate the result
        // without leaking back into the source list.
        expect(filterDomainsByOwner(domains, [])).not.toBe(domains);
    });

    it('keeps only domains whose owners intersect the selection', () => {
        const ownedByJane = makeDomain([makeOwner(jane)]);
        const ownedByJohn = makeDomain([makeOwner(john)]);
        const ownedByBoth = makeDomain([makeOwner(jane), makeOwner(john)]);

        const result = filterDomainsByOwner([ownedByJane, ownedByJohn, ownedByBoth], ['urn:li:corpuser:jane']);

        expect(result).toEqual([ownedByJane, ownedByBoth]);
    });

    it('drops every domain when no domain matches the selection', () => {
        const ownedByJane = makeDomain([makeOwner(jane)]);
        const result = filterDomainsByOwner([ownedByJane], ['urn:li:corpuser:not-an-owner']);
        expect(result).toEqual([]);
    });
});
