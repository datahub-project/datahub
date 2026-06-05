import { renderHook } from '@testing-library/react-hooks';

import {
    buildTermTreeOptions,
    groupGlossaryEntitiesByRoot,
    partitionGroupByDirectParent,
    useTermTreeOptions,
} from '@app/shared/tags/useTermTreeOptions';

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

const makeNode = (
    urn: string,
    name: string,
    parents: GlossaryNode[] = [],
    colorHex?: string,
    childrenCount?: { termsCount?: number; nodesCount?: number },
): GlossaryNode => {
    const parentNodes: ParentNodesResult = { count: parents.length, nodes: parents };
    return {
        urn,
        type: EntityType.GlossaryNode,
        properties: { name },
        displayProperties: colorHex ? { colorHex } : undefined,
        parentNodes,
        ...(childrenCount ? { childrenCount } : {}),
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

    it('propagates the root color to descendant node headers (matches the sidebar)', () => {
        // Regression: `Adoption.PetProperties` used to render with its own URN-hashed color
        // (e.g. orange) in the modal, while the sidebar showed it with Adoption's color (pink)
        // because `NodeItem` passes `iconColor` recursively. The two views must agree, so
        // intermediate node headers in a chain inherit from the topmost ancestor.
        const adoption = makeNode('urn:li:glossaryNode:adoption', 'Adoption');
        const petProperties = makeNode('urn:li:glossaryNode:petProperties', 'PetProperties', [adoption]);
        const petName = makeTerm('urn:li:glossaryTerm:petName', 'PetName', [petProperties, adoption]);

        const { result } = renderHook(() => useTermTreeOptions({ entities: [adoption, petProperties, petName] }));

        const [adoptionRow, petPropertiesRow, petNameRow] = result.current.allOptions;
        const expectedColor = `color-for-${adoption.urn}`;
        expect(adoptionRow.color).toBe(expectedColor);
        expect(petPropertiesRow.color).toBe(expectedColor);
        expect(petNameRow.color).toBe(expectedColor);
    });

    it('a descendant node ignores its own colorHex when its ancestor sets the subtree color', () => {
        // Mirrors the sidebar's `iconColor || node.displayProperties.colorHex` precedence:
        // once a root is picked, its color wins even if a descendant has its own colorHex.
        const ROOT_COLOR = 'fixture-root-color';
        const CHILD_COLOR = 'fixture-child-color'; // Should be ignored.
        const adoption = makeNode('urn:li:glossaryNode:adoption', 'Adoption', [], ROOT_COLOR);
        const petProperties = makeNode('urn:li:glossaryNode:petProperties', 'PetProperties', [adoption], CHILD_COLOR);

        const { result } = renderHook(() => useTermTreeOptions({ entities: [adoption, petProperties] }));

        const [, petPropertiesRow] = result.current.allOptions;
        expect(petPropertiesRow.color).toBe(ROOT_COLOR);
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

    it('does not mark a node empty when its childrenCount reports zero on both axes', () => {
        // Regression: nodes the GraphQL fragment knows are leaves (e.g. an empty term group like
        // "Leads") used to get `isEmptyNode: true` because no children were in the current entity
        // list yet — which made the modal render a caret the sidebar does not.
        const leaf = makeNode('urn:li:glossaryNode:leaf', 'Leads', [], undefined, {
            termsCount: 0,
            nodesCount: 0,
        });

        const { result } = renderHook(() => useTermTreeOptions({ entities: [leaf] }));

        expect(result.current.allOptions).toHaveLength(1);
        expect(result.current.allOptions[0].isEmptyNode).toBeFalsy();
    });

    it('still marks a node empty when childrenCount reports children on either axis', () => {
        // A node with non-zero termsCount or nodesCount is expandable — the caret must stay
        // visible so the user can trigger the lazy fetch.
        const withTerms = makeNode('urn:li:glossaryNode:hasTerms', 'HasTerms', [], undefined, {
            termsCount: 3,
            nodesCount: 0,
        });
        const withNodes = makeNode('urn:li:glossaryNode:hasNodes', 'HasNodes', [], undefined, {
            termsCount: 0,
            nodesCount: 2,
        });

        const { result } = renderHook(() => useTermTreeOptions({ entities: [withTerms, withNodes] }));

        expect(result.current.allOptions[0].isEmptyNode).toBe(true);
        expect(result.current.allOptions[1].isEmptyNode).toBe(true);
    });

    it('falls back to marking a node empty when childrenCount is missing (unknown)', () => {
        // We can't prove a node is a leaf without `childrenCount`, so the safe default is to
        // keep the caret visible — clicking it will issue a fetch and resolve the unknown.
        const unknown = makeNode('urn:li:glossaryNode:unknown', 'Unknown');

        const { result } = renderHook(() => useTermTreeOptions({ entities: [unknown] }));

        expect(result.current.allOptions[0].isEmptyNode).toBe(true);
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

    it('inserts an inline loading placeholder under a node whose child fetch is in flight', () => {
        // When the modal expands a node, `useGlossaryTreeEntities` tracks it in `fetchingNodes`
        // until the scrollAcrossEntities query resolves. During that window, the hook should
        // surface a synthetic loading row right under the node, not a global spinner.
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const { result } = renderHook(() =>
            useTermTreeOptions({
                entities: [root],
                loadingNodeUrns: new Set(['urn:li:glossaryNode:root']),
            }),
        );

        const labels = result.current.allOptions.map((o) => o.label);
        expect(labels).toEqual(['Root', 'Loading']);
        const loadingRow = result.current.allOptions[1];
        expect(loadingRow.isLoadingPlaceholder).toBe(true);
        expect(loadingRow.depth).toBe(1);
        expect(loadingRow.ancestorUrns).toEqual(['urn:li:glossaryNode:root']);
        // Loading rows must not be selectable.
        expect(result.current.disabledValues).toContain(loadingRow.value);
    });

    it('drops the loading placeholder once the node has fetched children', () => {
        // Race-condition guard: if `fetchingNodes` is still populated in the same render where
        // the children arrive, we shouldn't render both the loader and the freshly-loaded
        // children. Once any child is in the entities list, the loader is no longer needed.
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const child = makeTerm('urn:li:glossaryTerm:c1', 'Child1', [root]);
        const { result } = renderHook(() =>
            useTermTreeOptions({
                entities: [root, child],
                loadingNodeUrns: new Set(['urn:li:glossaryNode:root']),
            }),
        );

        const labels = result.current.allOptions.map((o) => o.label);
        expect(labels).toEqual(['Root', 'Child1']);
    });

    it('emits descendants in depth-first order so children sit directly under their parent node', () => {
        // Regression: when a parent node has both a deep-nested branch and a sibling leaf, the
        // deep branch's children used to render *after* the sibling because they came later in
        // the entities array. The hook now walks each subtree DFS so the parent → child
        // relationship is visually preserved.
        const adoption = makeNode('urn:li:glossaryNode:adoption', 'Adoption');
        const petProperties = makeNode('urn:li:glossaryNode:petProperties', 'PetProperties', [adoption]);
        const daysInStatus = makeTerm('urn:li:glossaryTerm:daysInStatus', 'DaysInStatus', [adoption]);
        const petName = makeTerm('urn:li:glossaryTerm:petName', 'PetName', [petProperties, adoption]);

        const { result } = renderHook(() =>
            useTermTreeOptions({ entities: [adoption, petProperties, daysInStatus, petName] }),
        );

        const labels = result.current.allOptions.map((o) => o.label);
        // PetName must follow PetProperties (its parent), not DaysInStatus (which was inserted earlier).
        expect(labels).toEqual(['Adoption', 'PetProperties', 'PetName', 'DaysInStatus']);
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

describe('groupGlossaryEntitiesByRoot', () => {
    it('preserves first-appearance order of roots while bucketing descendants', () => {
        const adoption = makeNode('urn:li:glossaryNode:adoption', 'Adoption');
        const classification = makeNode('urn:li:glossaryNode:classification', 'Classification');
        const adoptionTerm = makeTerm('urn:li:glossaryTerm:t1', 'AdoptionT', [adoption]);

        const { groupsByRoot, rootOrder } = groupGlossaryEntitiesByRoot(
            [adoption, classification, adoptionTerm],
            new Set(),
        );

        expect(rootOrder).toEqual([adoption.urn, classification.urn]);
        expect(groupsByRoot.get(adoption.urn)?.map((e) => e.urn)).toEqual([adoption.urn, adoptionTerm.urn]);
        expect(groupsByRoot.get(classification.urn)?.map((e) => e.urn)).toEqual([classification.urn]);
    });

    it('drops excluded URNs and non-glossary entities', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'T1', [root]);
        const nonGlossary = { urn: 'urn:li:dataset:1', type: EntityType.Dataset } as unknown as Entity;

        const { rootOrder } = groupGlossaryEntitiesByRoot([root, term, nonGlossary], new Set([term.urn]));

        expect(rootOrder).toEqual([root.urn]);
    });
});

describe('partitionGroupByDirectParent', () => {
    it('routes each entity to its direct parent and collects subgroup roots', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const mid = makeNode('urn:li:glossaryNode:mid', 'Mid', [root]);
        const t1 = makeTerm('urn:li:glossaryTerm:t1', 'T1', [mid, root]);
        // Entity whose direct parent isn't in the group (a search-path orphan) becomes a subgroup root.
        const orphan = makeTerm('urn:li:glossaryTerm:orphan', 'Orphan', [
            makeNode('urn:li:glossaryNode:notInGroup', 'NotInGroup'),
        ]);

        const { childMap, subgroupRoots } = partitionGroupByDirectParent([root, mid, t1, orphan]);

        expect(subgroupRoots.map((e) => e.urn)).toEqual([root.urn, orphan.urn]);
        expect(childMap.get(root.urn)?.map((e) => e.urn)).toEqual([mid.urn]);
        expect(childMap.get(mid.urn)?.map((e) => e.urn)).toEqual([t1.urn]);
    });
});

describe('buildTermTreeOptions (pure)', () => {
    // Direct invocation of the pure builder — no hook context required. Useful for asserting
    // that the dependency-injected `getDisplayName` / `getFallbackColor` are actually wired
    // through, and for guarding the contract the hook depends on without rendering React.
    const getDisplayName = (entity: GlossaryNode | GlossaryTerm) => (entity as any).properties?.name ?? entity.urn;
    const getFallbackColor = (urn: string) => `color-for-${urn}`;

    it('returns an empty list for an empty input', () => {
        expect(
            buildTermTreeOptions({
                entities: [],
                excludeSet: new Set(),
                loadingSet: new Set(),
                getDisplayName,
                getFallbackColor,
            }),
        ).toEqual([]);
    });

    it('builds a DFS-ordered tree without needing a React context', () => {
        const root = makeNode('urn:li:glossaryNode:root', 'Root');
        const term = makeTerm('urn:li:glossaryTerm:t1', 'T1', [root]);
        const options = buildTermTreeOptions({
            entities: [root, term],
            excludeSet: new Set(),
            loadingSet: new Set(),
            getDisplayName,
            getFallbackColor,
        });
        expect(options.map((o) => o.label)).toEqual(['Root', 'T1']);
        expect(options[1].color).toBe(`color-for-${root.urn}`); // Term inherits root's color via fallback.
    });
});
