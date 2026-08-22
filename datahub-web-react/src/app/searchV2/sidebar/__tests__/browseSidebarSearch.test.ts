import {
    type BrowseSearchEntity,
    type BrowseSearchHit,
    applyBrowseSearchHit,
    browseSearchHitKey,
    browseSearchHitLocation,
    extractBrowseSearchHits,
    isBrowseSidebarSearchActive,
    nameMatchesQuery,
    withBrowsePathContainsFilter,
} from '@app/searchV2/sidebar/browseSidebarSearch';
import {
    BROWSE_PATH_V2_FILTER_NAME,
    ORIGIN_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    UNIT_SEPARATOR,
} from '@app/searchV2/utils/constants';

import { EntityType, FilterOperator } from '@types';

const BIGQUERY = 'urn:li:dataPlatform:bigquery';

function entity(partial: Partial<BrowseSearchEntity> & Pick<BrowseSearchEntity, 'urn' | 'name'>): BrowseSearchEntity {
    return {
        type: EntityType.Dataset,
        browsePath: [],
        ...partial,
    };
}

describe('browseSidebarSearch', () => {
    describe('isBrowseSidebarSearchActive', () => {
        it('activates on immediate input, not only trimmed-empty strings', () => {
            expect(isBrowseSidebarSearchActive('analytics')).toBe(true);
            expect(isBrowseSidebarSearchActive('  analytics  ')).toBe(true);
            expect(isBrowseSidebarSearchActive('')).toBe(false);
            expect(isBrowseSidebarSearchActive('   ')).toBe(false);
        });
    });

    describe('nameMatchesQuery', () => {
        it('matches case-insensitively as a substring', () => {
            expect(nameMatchesQuery('PlaywrightBrowseEntity', 'browse')).toBe(true);
            expect(nameMatchesQuery('analytics', 'AN')).toBe(true);
            expect(nameMatchesQuery('analytics', 'zzz')).toBe(false);
            expect(nameMatchesQuery(null, 'a')).toBe(false);
        });
    });

    describe('extractBrowseSearchHits', () => {
        it('extracts a platform hit from a matching platform name', () => {
            const hits = extractBrowseSearchHits(
                [
                    entity({
                        urn: 'urn:li:dataset:orders',
                        name: 'orders',
                        platform: { urn: BIGQUERY, name: 'BigQuery' },
                        browsePath: [{ name: 'proj' }],
                    }),
                ],
                'big',
            );

            expect(hits).toEqual([
                expect.objectContaining({
                    kind: 'platform',
                    label: 'BigQuery',
                    platformUrn: BIGQUERY,
                    path: [],
                }),
            ]);
        });

        it('extracts matching browse path segments at any depth', () => {
            const hits = extractBrowseSearchHits(
                [
                    entity({
                        urn: 'urn:li:dataset:orders',
                        name: 'orders',
                        platform: { urn: BIGQUERY, name: 'BigQuery' },
                        origin: 'PROD',
                        browsePath: [
                            { name: 'PlaywrightBrowseEntity' },
                            {
                                name: 'analytics',
                                entity: { urn: 'urn:li:container:analytics', type: EntityType.Container },
                            },
                            { name: 'test_schema' },
                        ],
                    }),
                ],
                'lytics',
            );

            expect(hits).toHaveLength(1);
            expect(hits[0]).toMatchObject({
                kind: 'path',
                label: 'analytics',
                platformUrn: BIGQUERY,
                origin: 'PROD',
                path: ['PlaywrightBrowseEntity', 'analytics'],
                entity: { urn: 'urn:li:container:analytics', type: EntityType.Container },
            });
        });

        it('treats a matching container as a browse node using its urn in the path', () => {
            const containerUrn = 'urn:li:container:analytics';
            const hits = extractBrowseSearchHits(
                [
                    entity({
                        urn: containerUrn,
                        type: EntityType.Container,
                        name: 'ANALYTICS',
                        platform: { urn: BIGQUERY, name: 'BigQuery' },
                        browsePath: [{ name: 'urn:li:container:parent' }],
                    }),
                ],
                'analytics',
            );

            expect(hits).toEqual([
                expect.objectContaining({
                    kind: 'path',
                    label: 'ANALYTICS',
                    path: ['urn:li:container:parent', containerUrn],
                    pathLabels: ['', 'ANALYTICS'],
                    entity: { urn: containerUrn, type: EntityType.Container },
                }),
            ]);
        });

        it('matches a container folder by display name while keeping the indexed urn path', () => {
            const containerUrn = 'urn:li:container:64a5f47928e5c808b19f1385be046bc0';
            const hits = extractBrowseSearchHits(
                [
                    entity({
                        urn: 'urn:li:dataset:orders',
                        name: 'orders',
                        platform: { urn: BIGQUERY, name: 'BigQuery' },
                        browsePath: [
                            {
                                name: containerUrn,
                                displayName: 'ANALYTICS',
                                entity: { urn: containerUrn, type: EntityType.Container },
                            },
                        ],
                    }),
                ],
                'ana',
            );

            expect(hits).toEqual([
                expect.objectContaining({
                    kind: 'path',
                    label: 'ANALYTICS',
                    path: [containerUrn],
                }),
            ]);
        });

        it('maps a matching leaf asset to its containing folder', () => {
            const hits = extractBrowseSearchHits(
                [
                    entity({
                        urn: 'urn:li:dataset:customers',
                        name: 'customers',
                        platform: { urn: BIGQUERY, name: 'BigQuery' },
                        browsePath: [{ name: 'PlaywrightBrowseEntity' }, { name: 'test_schema' }],
                    }),
                ],
                'customer',
            );

            expect(hits).toEqual([
                expect.objectContaining({
                    kind: 'entity',
                    label: 'customers',
                    path: ['PlaywrightBrowseEntity', 'test_schema'],
                }),
            ]);
        });

        it('dedupes identical path hits discovered via different entities', () => {
            const hits = extractBrowseSearchHits(
                [
                    entity({
                        urn: 'urn:li:dataset:one',
                        name: 'one',
                        platform: { urn: BIGQUERY, name: 'BigQuery' },
                        browsePath: [{ name: 'analytics' }],
                    }),
                    entity({
                        urn: 'urn:li:dataset:two',
                        name: 'two',
                        platform: { urn: BIGQUERY, name: 'BigQuery' },
                        browsePath: [{ name: 'analytics' }],
                    }),
                ],
                'analytics',
            );

            expect(hits).toHaveLength(1);
            expect(hits[0].kind).toBe('path');
        });

        it('sorts platforms before shallower paths before entity hits', () => {
            const hits = extractBrowseSearchHits(
                [
                    entity({
                        urn: 'urn:li:dataset:bigquery_orders',
                        name: 'bigquery_orders',
                        platform: { urn: BIGQUERY, name: 'BigQuery' },
                        browsePath: [{ name: 'bigquery_folder' }, { name: 'nested' }],
                    }),
                ],
                'bigquery',
            );

            expect(hits.map((hit) => hit.kind)).toEqual(['platform', 'path', 'entity']);
        });

        it('returns no hits when nothing matches', () => {
            expect(
                extractBrowseSearchHits(
                    [
                        entity({
                            urn: 'urn:li:dataset:orders',
                            name: 'orders',
                            platform: { urn: BIGQUERY, name: 'Snowflake' },
                            browsePath: [{ name: 'public' }],
                        }),
                    ],
                    'analytics',
                ),
            ).toEqual([]);
        });
    });

    describe('applyBrowseSearchHit', () => {
        const tags = { field: TAGS_FILTER_NAME, values: ['urn:li:tag:pii'] };

        it('applies platform and path filters and keeps unrelated facets', () => {
            const hit: BrowseSearchHit = {
                key: 'path',
                kind: 'path',
                label: 'analytics',
                platformUrn: BIGQUERY,
                origin: 'PROD',
                path: ['proj', 'analytics'],
            };

            expect(applyBrowseSearchHit(hit, [tags])).toEqual([
                tags,
                {
                    field: PLATFORM_FILTER_NAME,
                    condition: FilterOperator.Equal,
                    values: [BIGQUERY],
                },
                {
                    field: ORIGIN_FILTER_NAME,
                    condition: FilterOperator.Equal,
                    values: ['PROD'],
                },
                {
                    field: BROWSE_PATH_V2_FILTER_NAME,
                    condition: FilterOperator.Equal,
                    values: [`${UNIT_SEPARATOR}proj${UNIT_SEPARATOR}analytics`],
                },
            ]);
        });

        it('clears the browse path when selecting a platform-only hit', () => {
            const hit: BrowseSearchHit = {
                key: 'platform',
                kind: 'platform',
                label: 'BigQuery',
                platformUrn: BIGQUERY,
                path: [],
            };

            expect(
                applyBrowseSearchHit(hit, [
                    tags,
                    { field: BROWSE_PATH_V2_FILTER_NAME, values: [`${UNIT_SEPARATOR}old`] },
                    { field: PLATFORM_FILTER_NAME, values: ['urn:li:dataPlatform:snowflake'] },
                ]),
            ).toEqual([
                tags,
                {
                    field: PLATFORM_FILTER_NAME,
                    condition: FilterOperator.Equal,
                    values: [BIGQUERY],
                },
            ]);
        });
    });

    describe('withBrowsePathContainsFilter', () => {
        it('adds a contain filter so nested folders can be found without expanding the tree', () => {
            expect(withBrowsePathContainsFilter([], 'analytics')).toEqual([
                {
                    and: [
                        {
                            field: BROWSE_PATH_V2_FILTER_NAME,
                            condition: FilterOperator.Contain,
                            values: ['analytics'],
                        },
                    ],
                },
            ]);
        });

        it('replaces an existing browse path filter inside each OR group', () => {
            expect(
                withBrowsePathContainsFilter(
                    [
                        {
                            and: [
                                { field: TAGS_FILTER_NAME, values: ['urn:li:tag:pii'] },
                                { field: BROWSE_PATH_V2_FILTER_NAME, values: [`${UNIT_SEPARATOR}old`] },
                            ],
                        },
                    ],
                    'analytics',
                ),
            ).toEqual([
                {
                    and: [
                        { field: TAGS_FILTER_NAME, values: ['urn:li:tag:pii'] },
                        {
                            field: BROWSE_PATH_V2_FILTER_NAME,
                            condition: FilterOperator.Contain,
                            values: ['analytics'],
                        },
                    ],
                },
            ]);
        });
    });

    describe('browseSearchHitLocation', () => {
        it('names the platform and parent folders for an entity hit', () => {
            expect(
                browseSearchHitLocation({
                    kind: 'entity',
                    platformName: 'Snowflake',
                    pathLabels: ['ANALYTICS', 'public'],
                }),
            ).toBe('Snowflake / ANALYTICS / public');
        });

        it('shows only parent folders for a path hit (platform is the row icon)', () => {
            expect(
                browseSearchHitLocation({
                    kind: 'path',
                    platformName: 'Snowflake',
                    pathLabels: ['PlaywrightBrowseEntity', 'ANALYTICS'],
                }),
            ).toBe('PlaywrightBrowseEntity');
        });

        it('is empty for a top-level folder or platform', () => {
            expect(
                browseSearchHitLocation({
                    kind: 'path',
                    platformName: 'Snowflake',
                    pathLabels: ['ANALYTICS'],
                }),
            ).toBe('');
            expect(browseSearchHitLocation({ kind: 'platform', platformName: 'Snowflake' })).toBe('');
        });
    });

    describe('browseSearchHitKey', () => {
        it('is stable for the same node', () => {
            const partial = {
                kind: 'path' as const,
                label: 'analytics',
                platformUrn: BIGQUERY,
                path: ['proj', 'analytics'],
            };
            expect(browseSearchHitKey(partial)).toBe(browseSearchHitKey(partial));
        });
    });
});
