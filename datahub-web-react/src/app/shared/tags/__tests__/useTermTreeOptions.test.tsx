import { act } from '@testing-library/react';
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

const makeNode = (urn: string, name: string, colorHex?: string): GlossaryNode =>
    ({
        urn,
        type: EntityType.GlossaryNode,
        properties: { name },
        displayProperties: colorHex ? { colorHex } : undefined,
    }) as unknown as GlossaryNode;

const makeParents = (nodes: GlossaryNode[]): ParentNodesResult => ({ count: nodes.length, nodes });

const makeTerm = (urn: string, name: string, parents: GlossaryNode[] = []): GlossaryTerm =>
    ({
        urn,
        type: EntityType.GlossaryTerm,
        properties: { name },
        parentNodes: makeParents(parents),
    }) as unknown as GlossaryTerm;

describe('useTermTreeOptions', () => {
    beforeEach(() => {
        // Display name = the .properties.name field for our synthetic entities.
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
        const rootWithColor = makeNode('urn:li:glossaryNode:root', 'Root', '#abcdef');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [rootWithColor]);
        const { result } = renderHook(() => useTermTreeOptions({ entities: [term] }));

        const [rootRow, termRow] = result.current.allOptions;
        expect(rootRow.color).toBe('#abcdef');
        // Term rows inherit the root node's color (so all terms in a lineage share one color).
        expect(termRow.color).toBe('#abcdef');
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

    it('hides descendant rows when a node is collapsed and `ignoreCollapsed` is false', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [root]);
        const { result } = renderHook(() => useTermTreeOptions({ entities: [term] }));

        expect(result.current.visibleOptions.map((o) => o.label)).toEqual(['Root', 'Term1']);

        act(() => result.current.toggleNodeCollapsed('urn:li:glossaryNode:root'));
        expect(result.current.visibleOptions.map((o) => o.label)).toEqual(['Root']);

        act(() => result.current.toggleNodeCollapsed('urn:li:glossaryNode:root'));
        expect(result.current.visibleOptions.map((o) => o.label)).toEqual(['Root', 'Term1']);
    });

    it('keeps descendants visible when `ignoreCollapsed` is true (search mode)', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [root]);
        const { result } = renderHook(() => useTermTreeOptions({ entities: [term], ignoreCollapsed: true }));

        act(() => result.current.toggleNodeCollapsed('urn:li:glossaryNode:root'));
        // Even with the root collapsed, ignoreCollapsed forces all rows to stay visible.
        expect(result.current.visibleOptions.map((o) => o.label)).toEqual(['Root', 'Term1']);
    });

    it('marks only nodes that have at least one descendant in `nodesWithChildren`', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const childlessNode = makeNode('urn:li:glossaryNode:standalone', 'Standalone');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'Term1', [root]);

        // childlessNode is not on any term's parent chain, so it never gets emitted at all.
        // root has a descendant term → it's in nodesWithChildren via the term's ancestorUrns chain.
        const { result } = renderHook(() =>
            useTermTreeOptions({ entities: [term, childlessNode as unknown as GlossaryTerm] }),
        );

        expect(result.current.nodesWithChildren.has('urn:li:glossaryNode:root')).toBe(true);
        expect(result.current.nodesWithChildren.has('urn:li:glossaryNode:standalone')).toBe(false);
    });
});
