import { describe, expect, it } from 'vitest';

import { DocumentCreator, DocumentTreeNode } from '@app/document/DocumentTreeContext';
import {
    DATAHUB_PLATFORM_URN,
    DEFAULT_STATUS_FILTER,
    filterDocumentNodes,
    getAvailablePlatforms,
    getDistinctCreators,
    matchesDocumentFilter,
} from '@app/document/utils/documentTreeFilters';

import { DataPlatform, EntityType } from '@types';

// Minimal platform fixtures — only the fields the filter util reads.
const NOTION = { urn: 'urn:li:dataPlatform:notion' } as DataPlatform;
const GITHUB = { urn: 'urn:li:dataPlatform:github' } as DataPlatform;
const DATAHUB = { urn: 'urn:li:dataPlatform:datahub' } as DataPlatform;

// Minimal creator fixtures. Two humans cover the dedup + allow-list cases for
// OSS — agents aren't represented as a separate concept here, so unlike SaaS
// there's no agent fixture.
const JANE: DocumentCreator = {
    urn: 'urn:li:corpuser:jane',
    type: EntityType.CorpUser,
    displayName: 'Jane Doe',
    pictureLink: null,
};
const JOHN: DocumentCreator = {
    urn: 'urn:li:corpuser:john',
    type: EntityType.CorpUser,
    displayName: 'John Smith',
    pictureLink: null,
};

function makeNode(overrides: Partial<DocumentTreeNode> = {}): DocumentTreeNode {
    return {
        urn: 'urn:li:document:test',
        title: 'Test Document',
        parentUrn: null,
        hasChildren: false,
        children: undefined,
        isUnpublished: false,
        isExternal: false,
        platform: null,
        creator: null,
        ...overrides,
    };
}

// Spread-in defaults used across the matchesDocumentFilter tests to keep each
// case focused on the dimension under test.
const DEFAULTS = {
    status: DEFAULT_STATUS_FILTER,
    selectedAuthorUrns: null,
    selectedPlatformUrns: null,
} as const;

describe('documentTreeFilters', () => {
    describe('getAvailablePlatforms', () => {
        it('should return an empty array for an empty list of nodes', () => {
            expect(getAvailablePlatforms([])).toEqual([]);
        });

        it('should return a single platform when all nodes share the same platform', () => {
            const nodes = [makeNode({ urn: 'a', platform: NOTION }), makeNode({ urn: 'b', platform: NOTION })];
            expect(getAvailablePlatforms(nodes)).toEqual([NOTION]);
        });

        it('should dedupe platforms by URN and preserve first-occurrence order', () => {
            const nodes = [
                makeNode({ urn: 'a', platform: GITHUB }),
                makeNode({ urn: 'b', platform: NOTION }),
                makeNode({ urn: 'c', platform: GITHUB }), // duplicate
                makeNode({ urn: 'd', platform: DATAHUB }),
            ];
            expect(getAvailablePlatforms(nodes)).toEqual([GITHUB, NOTION, DATAHUB]);
        });

        it('should skip nodes that have no platform', () => {
            const nodes = [
                makeNode({ urn: 'a', platform: null }),
                makeNode({ urn: 'b', platform: GITHUB }),
                makeNode({ urn: 'c' }), // platform undefined via default
            ];
            expect(getAvailablePlatforms(nodes)).toEqual([GITHUB]);
        });

        it('should skip nodes whose platform has no URN', () => {
            const nodes = [
                makeNode({ urn: 'a', platform: { urn: '' } as DataPlatform }),
                makeNode({ urn: 'b', platform: GITHUB }),
            ];
            expect(getAvailablePlatforms(nodes)).toEqual([GITHUB]);
        });
    });

    describe('getDistinctCreators', () => {
        it('should return an empty array for an empty list', () => {
            expect(getDistinctCreators([])).toEqual([]);
        });

        it('should dedupe creators by URN and preserve first-occurrence order', () => {
            const nodes = [
                makeNode({ urn: 'a', creator: JANE }),
                makeNode({ urn: 'b', creator: JOHN }),
                makeNode({ urn: 'c', creator: JANE }), // dup
            ];
            expect(getDistinctCreators(nodes).map((c) => c.urn)).toEqual([JANE.urn, JOHN.urn]);
        });

        it('should skip nodes with no creator', () => {
            const nodes = [makeNode({ urn: 'a', creator: null }), makeNode({ urn: 'b', creator: JANE })];
            expect(getDistinctCreators(nodes)).toEqual([JANE]);
        });
    });

    describe('matchesDocumentFilter', () => {
        describe('status filter', () => {
            it("should accept any status when filter is 'all'", () => {
                expect(matchesDocumentFilter(makeNode({ isUnpublished: false }), { ...DEFAULTS, status: 'all' })).toBe(
                    true,
                );
                expect(matchesDocumentFilter(makeNode({ isUnpublished: true }), { ...DEFAULTS, status: 'all' })).toBe(
                    true,
                );
            });

            it("should reject unpublished nodes when filter is 'published'", () => {
                expect(
                    matchesDocumentFilter(makeNode({ isUnpublished: true }), { ...DEFAULTS, status: 'published' }),
                ).toBe(false);
                expect(
                    matchesDocumentFilter(makeNode({ isUnpublished: false }), { ...DEFAULTS, status: 'published' }),
                ).toBe(true);
            });

            it("should reject published nodes when filter is 'unpublished'", () => {
                expect(
                    matchesDocumentFilter(makeNode({ isUnpublished: false }), { ...DEFAULTS, status: 'unpublished' }),
                ).toBe(false);
                expect(
                    matchesDocumentFilter(makeNode({ isUnpublished: true }), { ...DEFAULTS, status: 'unpublished' }),
                ).toBe(true);
            });

            it("should treat undefined isUnpublished as published (filter='published' allows it)", () => {
                const node = makeNode();
                delete node.isUnpublished;
                expect(matchesDocumentFilter(node, { ...DEFAULTS, status: 'published' })).toBe(true);
                expect(matchesDocumentFilter(node, { ...DEFAULTS, status: 'unpublished' })).toBe(false);
            });
        });

        describe('author filter', () => {
            it('should accept any author when selectedAuthorUrns is null', () => {
                expect(matchesDocumentFilter(makeNode({ creator: JANE }), DEFAULTS)).toBe(true);
                expect(matchesDocumentFilter(makeNode({ creator: null }), DEFAULTS)).toBe(true);
            });

            it('should accept any author when selectedAuthorUrns is an empty array', () => {
                expect(
                    matchesDocumentFilter(makeNode({ creator: JANE }), { ...DEFAULTS, selectedAuthorUrns: [] }),
                ).toBe(true);
            });

            it('should accept nodes whose creator URN is in the allow-list', () => {
                expect(
                    matchesDocumentFilter(makeNode({ creator: JANE }), {
                        ...DEFAULTS,
                        selectedAuthorUrns: [JANE.urn, JOHN.urn],
                    }),
                ).toBe(true);
            });

            it('should reject nodes whose creator URN is not in the allow-list', () => {
                expect(
                    matchesDocumentFilter(makeNode({ creator: JANE }), {
                        ...DEFAULTS,
                        selectedAuthorUrns: [JOHN.urn],
                    }),
                ).toBe(false);
            });

            it('should reject nodes with no creator when an allow-list is active', () => {
                expect(
                    matchesDocumentFilter(makeNode({ creator: null }), {
                        ...DEFAULTS,
                        selectedAuthorUrns: [JANE.urn],
                    }),
                ).toBe(false);
            });
        });

        describe('source filter', () => {
            it('should accept any source when selectedPlatformUrns is null', () => {
                expect(matchesDocumentFilter(makeNode({ platform: GITHUB }), { ...DEFAULTS })).toBe(true);
                expect(matchesDocumentFilter(makeNode({ platform: null }), { ...DEFAULTS })).toBe(true);
            });

            it('should accept any source when selectedPlatformUrns is an empty array', () => {
                expect(
                    matchesDocumentFilter(makeNode({ platform: GITHUB }), { ...DEFAULTS, selectedPlatformUrns: [] }),
                ).toBe(true);
            });

            it('should accept nodes whose platform URN is in the allow-list', () => {
                expect(
                    matchesDocumentFilter(makeNode({ platform: GITHUB, isExternal: true }), {
                        ...DEFAULTS,
                        selectedPlatformUrns: [GITHUB.urn, NOTION.urn],
                    }),
                ).toBe(true);
            });

            it('should reject nodes whose platform URN is not in the allow-list', () => {
                expect(
                    matchesDocumentFilter(makeNode({ platform: GITHUB, isExternal: true }), {
                        ...DEFAULTS,
                        selectedPlatformUrns: [NOTION.urn],
                    }),
                ).toBe(false);
            });

            it('should reject nodes with no platform when an allow-list is active', () => {
                expect(
                    matchesDocumentFilter(makeNode({ platform: null }), {
                        ...DEFAULTS,
                        selectedPlatformUrns: [NOTION.urn],
                    }),
                ).toBe(false);
            });

            it('should accept native DataHub nodes by their platform URN', () => {
                expect(
                    matchesDocumentFilter(makeNode({ isExternal: false, platform: DATAHUB }), {
                        ...DEFAULTS,
                        selectedPlatformUrns: [DATAHUB_PLATFORM_URN],
                    }),
                ).toBe(true);
            });

            it('should reject external nodes when only the DataHub platform is selected', () => {
                expect(
                    matchesDocumentFilter(makeNode({ isExternal: true, platform: GITHUB }), {
                        ...DEFAULTS,
                        selectedPlatformUrns: [DATAHUB_PLATFORM_URN],
                    }),
                ).toBe(false);
            });

            it('should accept either DataHub or a specific external platform when both are in the allow-list', () => {
                const native = makeNode({ urn: 'a', isExternal: false, platform: DATAHUB });
                const github = makeNode({ urn: 'b', isExternal: true, platform: GITHUB });
                const notion = makeNode({ urn: 'c', isExternal: true, platform: NOTION });

                const selection = { ...DEFAULTS, selectedPlatformUrns: [DATAHUB_PLATFORM_URN, GITHUB.urn] };

                expect(matchesDocumentFilter(native, selection)).toBe(true);
                expect(matchesDocumentFilter(github, selection)).toBe(true);
                expect(matchesDocumentFilter(notion, selection)).toBe(false);
            });
        });

        it('should require status, author, and source to all pass when combined', () => {
            const node = makeNode({
                isUnpublished: true,
                isExternal: true,
                platform: GITHUB,
                creator: JANE,
            });

            // All three pass
            expect(
                matchesDocumentFilter(node, {
                    status: 'unpublished',
                    selectedAuthorUrns: [JANE.urn],
                    selectedPlatformUrns: [GITHUB.urn],
                }),
            ).toBe(true);

            // Status fails
            expect(
                matchesDocumentFilter(node, {
                    status: 'published',
                    selectedAuthorUrns: [JANE.urn],
                    selectedPlatformUrns: [GITHUB.urn],
                }),
            ).toBe(false);

            // Author fails — Jane isn't in the allow-list
            expect(
                matchesDocumentFilter(node, {
                    status: 'unpublished',
                    selectedAuthorUrns: [JOHN.urn],
                    selectedPlatformUrns: [GITHUB.urn],
                }),
            ).toBe(false);

            // Source fails
            expect(
                matchesDocumentFilter(node, {
                    status: 'unpublished',
                    selectedAuthorUrns: [JANE.urn],
                    selectedPlatformUrns: [NOTION.urn],
                }),
            ).toBe(false);
        });
    });

    describe('filterDocumentNodes', () => {
        it('should return only nodes that match all filters, preserving order', () => {
            const nodes = [
                makeNode({ urn: 'a', isUnpublished: false, isExternal: true, platform: GITHUB }),
                makeNode({ urn: 'b', isUnpublished: true, isExternal: true, platform: GITHUB }),
                makeNode({ urn: 'c', isUnpublished: false, isExternal: true, platform: NOTION }),
                makeNode({ urn: 'd', isUnpublished: true, isExternal: true, platform: NOTION }),
            ];

            const result = filterDocumentNodes(nodes, {
                ...DEFAULTS,
                status: 'unpublished',
                selectedPlatformUrns: [NOTION.urn],
            });

            expect(result.map((n) => n.urn)).toEqual(['d']);
        });

        it('should return the full input (in order) when no filters are active', () => {
            const nodes = [makeNode({ urn: 'a', platform: GITHUB }), makeNode({ urn: 'b', platform: NOTION })];

            const result = filterDocumentNodes(nodes, { ...DEFAULTS });

            expect(result.map((n) => n.urn)).toEqual(['a', 'b']);
        });

        it('should return an empty array when no nodes match', () => {
            const nodes = [makeNode({ urn: 'a', isUnpublished: false })];
            const result = filterDocumentNodes(nodes, { ...DEFAULTS, status: 'unpublished' });
            expect(result).toEqual([]);
        });
    });
});
