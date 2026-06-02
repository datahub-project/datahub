import { renderHook } from '@testing-library/react-hooks';

import { useTermTreeOptions } from '@app/shared/tags/useTermTreeOptions';

import { Entity, EntityType, GlossaryNode, GlossaryTerm, ParentNodesResult } from '@types';

const { mockUseGenerateGlossaryColorFromPalette, mockUseEntityRegistry } = vi.hoisted(() => ({
    mockUseGenerateGlossaryColorFromPalette: vi.fn(),
    mockUseEntityRegistry: vi.fn(),
}));

vi.mock('@app/glossaryV2/colorUtils', () => ({
    useGenerateGlossaryColorFromPalette: mockUseGenerateGlossaryColorFromPalette,
}));

vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: mockUseEntityRegistry,
}));

const makeNode = (urn: string, name: string, parents: GlossaryNode[] = [], colorHex?: string): GlossaryNode => {
    const parentNodes: ParentNodesResult = { count: parents.length, nodes: parents };
    return {
        urn,
        type: EntityType.GlossaryNode,
        properties: { name },
        displayProperties: colorHex ? { colorHex } : undefined,
        parentNodes,
    } as unknown as GlossaryNode;
};

const makeTerm = (urn: string, name: string, parents: GlossaryNode[] = []): GlossaryTerm => {
    const parentNodes: ParentNodesResult = { count: parents.length, nodes: parents };
    return {
        urn,
        type: EntityType.GlossaryTerm,
        properties: { name },
        parentNodes,
    } as unknown as GlossaryTerm;
};

describe('useTermTreeOptions', () => {
    beforeEach(() => {
        mockUseEntityRegistry.mockReturnValue({
            getDisplayName: (_type: EntityType, entity: Entity) => (entity as any).properties?.name ?? entity.urn,
        });
        mockUseGenerateGlossaryColorFromPalette.mockReturnValue((urn: string) => `color-for-${urn}`);
    });

    it('returns an empty tree for an empty entity list', () => {
        const { result } = renderHook(() => useTermTreeOptions({ entities: [] }));
        expect(result.current.allOptions).toEqual([]);
        expect(result.current.visibleOptions).toEqual([]);
        expect(result.current.disabledValues).toEqual([]);
        expect(result.current.nodesWithChildren.size).toBe(0);
    });

    it('emits a single term row (no nodes) when the term has no parents', () => {
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1');
        const { result } = renderHook(() => useTermTreeOptions({ entities: [term] }));
        expect(result.current.allOptions).toHaveLength(1);
        expect(result.current.allOptions[0]).toMatchObject({
            value: 'urn:li:glossaryTerm:t1',
            label: 'Term1',
            depth: 0,
        });
        expect(result.current.allOptions[0].isNode).toBeFalsy();
        expect(result.current.disabledValues).toEqual([]);
    });

    it('emits parent nodes root→leaf then the term row, reversing the direct-parent-first order', () => {
        // parentNodes is ordered direct-parent → root in GraphQL; the hook must reverse to root → leaf.
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const mid = makeNode('urn:li:glossaryNode:mid', 'Mid');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [mid, root]);

        const { result } = renderHook(() => useTermTreeOptions({ entities: [term] }));

        const labels = result.current.allOptions.map((o) => `${o.depth}/${o.label}/${o.isNode ? 'node' : 'term'}`);
        expect(labels).toEqual(['0/Root/node', '1/Mid/node', '2/Term1/term']);

        // Headers are disabled; only the term row is selectable.
        expect(result.current.disabledValues).toEqual(['urn:li:glossaryNode:root', 'urn:li:glossaryNode:mid']);
    });

    it('deduplicates shared ancestor nodes across two terms in the same lineage', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const t1 = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [root]);
        const t2 = makeTerm('urn:li:glossaryTerm:t2', 'Term2', [root]);

        const { result } = renderHook(() => useTermTreeOptions({ entities: [t1, t2] }));

        const labels = result.current.allOptions.map((o) => o.label);
        expect(labels).toEqual(['Root', 'Term1', 'Term2']);
        expect(result.current.disabledValues).toEqual(['urn:li:glossaryNode:root']);
    });

    it('uses node.displayProperties.colorHex when present, otherwise palette-derived color', () => {
        // The hook treats colorHex as an opaque pass-through string, so we use a non-hex sentinel
        // here to satisfy the `no-hardcoded-colors` lint rule (which forbids hex literals).
        const FIXTURE_COLOR = 'fixture-color-1';
        const rootWithColor = makeNode('urn:li:glossaryNode:root', 'Root', [], FIXTURE_COLOR);
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [rootWithColor]);
        const { result } = renderHook(() => useTermTreeOptions({ entities: [term] }));

        const [rootRow, termRow] = result.current.allOptions;
        expect(rootRow.color).toBe(FIXTURE_COLOR);
        // Term rows inherit the root node's color (so all terms in a lineage share one color).
        expect(termRow.color).toBe(FIXTURE_COLOR);
    });

    it('drops a term listed in excludeUrns and skips its lineage when all terms are excluded', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const t1 = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [root]);
        const t2 = makeTerm('urn:li:glossaryTerm:t2', 'Term2', [root]);

        const onlyOneExcluded = renderHook(() =>
            useTermTreeOptions({ entities: [t1, t2], excludeUrns: ['urn:li:glossaryTerm:t1'] }),
        );
        expect(onlyOneExcluded.result.current.allOptions.map((o) => o.label)).toEqual(['Root', 'Term2']);

        const allExcluded = renderHook(() =>
            useTermTreeOptions({
                entities: [t1, t2],
                excludeUrns: ['urn:li:glossaryTerm:t1', 'urn:li:glossaryTerm:t2'],
            }),
        );
        // No selectable children → the lineage's node header is not emitted either.
        expect(allExcluded.result.current.allOptions).toEqual([]);
    });

    it('hides descendant rows whose ancestors are not in `expandedNodes`', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [root]);

        const collapsed = renderHook(() => useTermTreeOptions({ entities: [term], expandedNodes: new Set() }));
        // Root is depth 0 (no ancestors) so it stays visible; the term is hidden until root expands.
        expect(collapsed.result.current.visibleOptions.map((o) => o.label)).toEqual(['Root']);

        const expanded = renderHook(() =>
            useTermTreeOptions({
                entities: [term],
                expandedNodes: new Set(['urn:li:glossaryNode:root']),
            }),
        );
        expect(expanded.result.current.visibleOptions.map((o) => o.label)).toEqual(['Root', 'Term1']);
    });

    it('shows every row when `expandedNodes` is undefined (search/flat mode)', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [root]);
        const { result } = renderHook(() => useTermTreeOptions({ entities: [term] }));
        expect(result.current.visibleOptions.map((o) => o.label)).toEqual(['Root', 'Term1']);
    });

    it('marks only nodes with at least one descendant row in `nodesWithChildren`', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [root]);
        const { result } = renderHook(() => useTermTreeOptions({ entities: [term] }));

        expect(result.current.nodesWithChildren.has('urn:li:glossaryNode:root')).toBe(true);
    });

    it('emits a standalone GlossaryNode entity as a header row (browse path before expansion)', () => {
        // A root node with no fetched children yet. The browse-path flow puts these in `entities`
        // so the user can see and click into them before any term has been pulled.
        const root = makeNode('urn:li:glossaryNode:root', 'Root');

        const { result } = renderHook(() => useTermTreeOptions({ entities: [root] }));

        expect(result.current.allOptions).toHaveLength(1);
        expect(result.current.allOptions[0]).toMatchObject({
            value: 'urn:li:glossaryNode:root',
            label: 'Root',
            depth: 0,
            isNode: true,
            isEmptyNode: true,
        });
        expect(result.current.disabledValues).toEqual(['urn:li:glossaryNode:root']);
    });

    it('renders a standalone GlossaryNode under its own ancestor chain', () => {
        // Mid is a child node whose own children haven't been fetched yet, but its parent chain
        // is known from the expand path.
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const mid = makeNode('urn:li:glossaryNode:mid', 'Mid', [root]);

        const { result } = renderHook(() => useTermTreeOptions({ entities: [mid] }));

        const labels = result.current.allOptions.map((o) => `${o.depth}/${o.label}`);
        expect(labels).toEqual(['0/Root', '1/Mid']);
        // Only the leaf (mid) is marked empty; the synthesized root ancestor isn't.
        expect(result.current.allOptions[0].isEmptyNode).toBeFalsy();
        expect(result.current.allOptions[1].isEmptyNode).toBe(true);
    });

    it('preserves the natural order of root nodes when one of them gets expanded with child terms', () => {
        // Regression: expanding a root used to "promote" it to the top of the list because
        // term-bearing lineages were emitted before standalone nodes. The order of root entities
        // in `entities` must drive the output regardless of which roots have children fetched.
        const rootA = makeNode('urn:li:glossaryNode:adoption', 'Adoption');
        const rootB = makeNode('urn:li:glossaryNode:classification', 'Classification');
        const rootC = makeNode('urn:li:glossaryNode:ecommerce', 'Ecommerce');
        const rootD = makeNode('urn:li:glossaryNode:personalInformation', 'PersonalInformation');
        const highRisk = makeTerm('urn:li:glossaryTerm:highRisk', 'HighRisk', [rootC]);
        const returnRate = makeTerm('urn:li:glossaryTerm:returnRate', 'ReturnRate', [rootC]);

        const { result } = renderHook(() =>
            useTermTreeOptions({ entities: [rootA, rootB, rootC, rootD, highRisk, returnRate] }),
        );

        const labels = result.current.allOptions.map((o) => o.label);
        expect(labels).toEqual([
            'Adoption',
            'Classification',
            'Ecommerce',
            'HighRisk',
            'ReturnRate',
            'PersonalInformation',
        ]);
    });

    it('skips a standalone GlossaryNode that already showed up as a term ancestor', () => {
        // Root is both passed in as a standalone node AND appears as a term's parent. It should
        // only be emitted once, not duplicated.
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [root]);

        const { result } = renderHook(() => useTermTreeOptions({ entities: [root, term] }));

        const labels = result.current.allOptions.map((o) => o.label);
        expect(labels).toEqual(['Root', 'Term1']);
        // Since a term was emitted under root, root is no longer "empty".
        expect(result.current.allOptions[0].isEmptyNode).toBeFalsy();
    });
});
