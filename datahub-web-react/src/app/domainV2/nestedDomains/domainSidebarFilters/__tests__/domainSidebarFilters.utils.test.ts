import { describe, expect, it } from 'vitest';

import { extractOwnerOptionsFromFacets } from '@app/domainV2/nestedDomains/domainSidebarFilters/domainSidebarFilters.utils';

import { CorpGroup, CorpUser, EntityType, FacetMetadata } from '@types';

// --- Fixtures ---------------------------------------------------------------
// `entityDisplayNameFields` returns CorpUser / CorpGroup with nested
// `properties` / `info` blocks but no `pictureLink`, so the fixtures mirror
// that shape. Cast through `unknown` so we only have to populate the fields
// the helper actually reads.

type FixtureOverrides = Record<string, unknown>;

function makeUser(overrides: FixtureOverrides = {}): CorpUser {
    return {
        urn: 'urn:li:corpuser:jane',
        type: EntityType.CorpUser,
        username: 'jane',
        properties: { displayName: 'Jane Doe' },
        ...overrides,
    } as unknown as CorpUser;
}

function makeGroup(overrides: FixtureOverrides = {}): CorpGroup {
    return {
        urn: 'urn:li:corpGroup:eng',
        type: EntityType.CorpGroup,
        name: 'engineering',
        properties: { displayName: 'Engineering' },
        ...overrides,
    } as unknown as CorpGroup;
}

function makeOwnersFacet(
    aggregations: ReadonlyArray<{ value: string; count: number; entity?: CorpUser | CorpGroup | null }>,
): FacetMetadata {
    return {
        field: 'owners',
        displayName: 'Owner',
        aggregations: aggregations.map((a) => ({
            value: a.value,
            count: a.count,
            entity: a.entity ?? null,
        })),
    } as unknown as FacetMetadata;
}

// --- Tests ------------------------------------------------------------------

describe('extractOwnerOptionsFromFacets', () => {
    it('returns an empty list when facets are null, undefined, or empty', () => {
        expect(extractOwnerOptionsFromFacets(null)).toEqual([]);
        expect(extractOwnerOptionsFromFacets(undefined)).toEqual([]);
        expect(extractOwnerOptionsFromFacets([])).toEqual([]);
    });

    it('returns an empty list when no "owners" facet is present', () => {
        const facets = [{ field: 'platform', displayName: 'Platform', aggregations: [] } as unknown as FacetMetadata];
        expect(extractOwnerOptionsFromFacets(facets)).toEqual([]);
    });

    it('projects CorpUser and CorpGroup aggregations into the owner-info shape', () => {
        const facets = [
            makeOwnersFacet([
                { value: 'urn:li:corpuser:jane', count: 3, entity: makeUser() },
                {
                    value: 'urn:li:corpGroup:eng',
                    count: 5,
                    entity: makeGroup(),
                },
            ]),
        ];

        expect(extractOwnerOptionsFromFacets(facets)).toEqual([
            { urn: 'urn:li:corpuser:jane', displayName: 'Jane Doe', type: EntityType.CorpUser },
            { urn: 'urn:li:corpGroup:eng', displayName: 'Engineering', type: EntityType.CorpGroup },
        ]);
    });

    it('skips aggregations whose entity is missing (schema marks entity as nullable)', () => {
        const facets = [
            makeOwnersFacet([
                { value: 'urn:li:corpuser:missing', count: 1, entity: null },
                { value: 'urn:li:corpuser:jane', count: 2, entity: makeUser() },
            ]),
        ];

        const result = extractOwnerOptionsFromFacets(facets);
        expect(result.map((o) => o.urn)).toEqual(['urn:li:corpuser:jane']);
    });

    it('skips entities that are neither CorpUser nor CorpGroup', () => {
        const facets = [
            makeOwnersFacet([
                {
                    value: 'urn:li:dataset:x',
                    count: 1,
                    entity: { urn: 'urn:li:dataset:x', type: EntityType.Dataset } as unknown as CorpUser,
                },
                { value: 'urn:li:corpuser:jane', count: 2, entity: makeUser() },
            ]),
        ];

        const result = extractOwnerOptionsFromFacets(facets);
        expect(result.map((o) => o.type)).toEqual([EntityType.CorpUser]);
    });

    it('falls back through displayName sources for users (editable → properties → info → fullName → username → urn)', () => {
        const facets = [
            makeOwnersFacet([
                {
                    value: 'urn:li:corpuser:editable',
                    count: 1,
                    entity: makeUser({
                        urn: 'urn:li:corpuser:editable',
                        editableProperties: { displayName: 'Editable Name' },
                        properties: { displayName: 'Properties Name' },
                    }),
                },
                {
                    value: 'urn:li:corpuser:info',
                    count: 1,
                    entity: makeUser({
                        urn: 'urn:li:corpuser:info',
                        properties: null,
                        info: { displayName: 'Info Name' },
                    }),
                },
                {
                    value: 'urn:li:corpuser:noname',
                    count: 1,
                    entity: makeUser({
                        urn: 'urn:li:corpuser:noname',
                        username: 'noname',
                        properties: null,
                        info: null,
                    }),
                },
                {
                    value: 'urn:li:corpuser:bare',
                    count: 1,
                    entity: makeUser({
                        urn: 'urn:li:corpuser:bare',
                        username: '',
                        properties: null,
                        info: null,
                    }),
                },
            ]),
        ];

        expect(extractOwnerOptionsFromFacets(facets).map((o) => o.displayName)).toEqual([
            'Editable Name',
            'Info Name',
            'noname',
            'urn:li:corpuser:bare',
        ]);
    });

    it('falls back to group `info.displayName` and `name` for groups without properties.displayName', () => {
        const facets = [
            makeOwnersFacet([
                {
                    value: 'urn:li:corpGroup:info',
                    count: 1,
                    entity: makeGroup({
                        urn: 'urn:li:corpGroup:info',
                        properties: null,
                        info: { displayName: 'Info Group' },
                    }),
                },
                {
                    value: 'urn:li:corpGroup:name',
                    count: 1,
                    entity: makeGroup({
                        urn: 'urn:li:corpGroup:name',
                        properties: null,
                        info: null,
                        name: 'name-group',
                    }),
                },
            ]),
        ];

        expect(extractOwnerOptionsFromFacets(facets).map((o) => o.displayName)).toEqual(['Info Group', 'name-group']);
    });
});
