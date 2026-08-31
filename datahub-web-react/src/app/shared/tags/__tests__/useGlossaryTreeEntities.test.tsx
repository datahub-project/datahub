import { act, renderHook } from '@testing-library/react-hooks';

import { useGlossaryTreeEntities } from '@app/shared/tags/useGlossaryTreeEntities';

import { EntityType } from '@types';

// Flushes pending promises so the `apolloClient.query(...).then(...).finally(...)` chain inside
// `expandNode` completes before assertions. `waitForNextUpdate` doesn't catch the post-finally
// state set when promises resolve synchronously from the mock — these explicit flushes do.
const flushPromises = () =>
    new Promise<void>((resolve) => {
        setTimeout(resolve, 0);
    });

// Apollo's `useApolloClient` is mocked so we can drive child-fetches deterministically without
// spinning up a `MockedProvider`. The hook's whole API surface is wrapped around `client.query`,
// so a single resolved-value mock per test gives us tight control over the lazy-expand flow.
const { mockUseApolloClient, mockUseGetRootGlossaryNodesQuery, mockUseGetRootGlossaryTermsQuery } = vi.hoisted(() => ({
    mockUseApolloClient: vi.fn(),
    mockUseGetRootGlossaryNodesQuery: vi.fn(),
    mockUseGetRootGlossaryTermsQuery: vi.fn(),
}));

vi.mock('@apollo/client', async () => {
    const actual = await vi.importActual<typeof import('@apollo/client')>('@apollo/client');
    return {
        ...actual,
        useApolloClient: mockUseApolloClient,
    };
});

vi.mock('@graphql/glossary.generated', () => ({
    useGetRootGlossaryNodesQuery: mockUseGetRootGlossaryNodesQuery,
    useGetRootGlossaryTermsQuery: mockUseGetRootGlossaryTermsQuery,
}));

const ROOT_NODE = {
    urn: 'urn:li:glossaryNode:root',
    type: EntityType.GlossaryNode,
    properties: { name: 'Root' },
};
const ROOT_TERM = {
    urn: 'urn:li:glossaryTerm:rootTerm',
    type: EntityType.GlossaryTerm,
    properties: { name: 'Root Term' },
};
const CHILD_NODE = {
    urn: 'urn:li:glossaryNode:child',
    type: EntityType.GlossaryNode,
    properties: { name: 'Child Node' },
};
const CHILD_TERM = {
    urn: 'urn:li:glossaryTerm:child',
    type: EntityType.GlossaryTerm,
    properties: { name: 'Child Term' },
};

function setupRootData(opts?: { nodes?: (typeof ROOT_NODE)[]; terms?: (typeof ROOT_TERM)[]; loading?: boolean }) {
    const nodes = opts?.nodes ?? [ROOT_NODE];
    const terms = opts?.terms ?? [ROOT_TERM];
    const loading = opts?.loading ?? false;
    mockUseGetRootGlossaryNodesQuery.mockReturnValue({
        data: { getRootGlossaryNodes: { nodes } },
        loading,
    });
    mockUseGetRootGlossaryTermsQuery.mockReturnValue({
        data: { getRootGlossaryTerms: { terms } },
        loading,
    });
}

describe('useGlossaryTreeEntities', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        setupRootData();
        mockUseApolloClient.mockReturnValue({ query: vi.fn() });
    });

    it('returns root nodes and root terms in the initial state with empty parent chains', () => {
        const { result } = renderHook(() => useGlossaryTreeEntities());
        expect(result.current.entities).toHaveLength(2);
        expect(result.current.entities[0]).toMatchObject({
            urn: ROOT_NODE.urn,
            parentNodes: { count: 0, nodes: [] },
        });
        expect(result.current.entities[1]).toMatchObject({
            urn: ROOT_TERM.urn,
            parentNodes: { count: 0, nodes: [] },
        });
        expect(result.current.expandedNodes.size).toBe(0);
    });

    it('reports loading until both root queries resolve', () => {
        setupRootData({ loading: true });
        const { result } = renderHook(() => useGlossaryTreeEntities());
        expect(result.current.isLoading).toBe(true);
    });

    it('flips expandedNodes immediately on expand and attaches the parent chain to fetched children', async () => {
        const query = vi.fn().mockResolvedValue({
            data: {
                scrollAcrossEntities: {
                    searchResults: [{ entity: CHILD_TERM }, { entity: CHILD_NODE }],
                },
            },
        });
        mockUseApolloClient.mockReturnValue({ query });

        const { result } = renderHook(() => useGlossaryTreeEntities());

        await act(async () => {
            result.current.expandNode(ROOT_NODE.urn);
        });
        // After the expand call returns, the caret is flipped open even before the network resolves.
        expect(result.current.expandedNodes.has(ROOT_NODE.urn)).toBe(true);

        await act(async () => {
            await flushPromises();
        });

        const childEntities = result.current.entities.filter((e) => [CHILD_TERM.urn, CHILD_NODE.urn].includes(e.urn));
        expect(childEntities).toHaveLength(2);
        // Synthesized parent chains are direct-parent-first (matches GraphQL convention).
        childEntities.forEach((e) => {
            expect((e as any).parentNodes.nodes).toHaveLength(1);
            expect((e as any).parentNodes.nodes[0].urn).toBe(ROOT_NODE.urn);
        });
        expect(query).toHaveBeenCalledTimes(1);
    });

    it('does not refetch children when re-expanding a previously-fetched node', async () => {
        const query = vi.fn().mockResolvedValue({
            data: { scrollAcrossEntities: { searchResults: [{ entity: CHILD_TERM }] } },
        });
        mockUseApolloClient.mockReturnValue({ query });

        const { result } = renderHook(() => useGlossaryTreeEntities());
        await act(async () => {
            result.current.expandNode(ROOT_NODE.urn);
        });
        await act(async () => {
            await flushPromises();
        });
        expect(query).toHaveBeenCalledTimes(1);

        // Collapse, then re-expand — cache should win, no second fetch.
        act(() => {
            result.current.collapseNode(ROOT_NODE.urn);
        });
        await act(async () => {
            result.current.expandNode(ROOT_NODE.urn);
        });
        expect(query).toHaveBeenCalledTimes(1);
        // Children remain visible because the cache repopulated the entities list on re-expand.
        expect(result.current.entities.some((e) => e.urn === CHILD_TERM.urn)).toBe(true);
    });

    it('keeps fetched children in `entityCache` even after the parent is collapsed', async () => {
        // Regression: the modal's chip strip used to fall back to raw URN strings ("urn:li:...")
        // for tree-picked terms whose parent was then collapsed, because the entity was gone from
        // `entities`. The cache must persist independent of expand state.
        const query = vi.fn().mockResolvedValue({
            data: { scrollAcrossEntities: { searchResults: [{ entity: CHILD_TERM }] } },
        });
        mockUseApolloClient.mockReturnValue({ query });

        const { result } = renderHook(() => useGlossaryTreeEntities());
        await act(async () => {
            result.current.expandNode(ROOT_NODE.urn);
        });
        await act(async () => {
            await flushPromises();
        });
        expect(result.current.entityCache[CHILD_TERM.urn]).toBeDefined();

        act(() => {
            result.current.collapseNode(ROOT_NODE.urn);
        });
        // entities loses the child (correctly hidden from the visible tree), but the cache holds
        // onto it so the chip strip can still resolve its display name.
        expect(result.current.entities.some((e) => e.urn === CHILD_TERM.urn)).toBe(false);
        expect(result.current.entityCache[CHILD_TERM.urn]).toBeDefined();
    });

    it('hides children of a collapsed node from the entities list (but keeps the cache)', async () => {
        const query = vi.fn().mockResolvedValue({
            data: { scrollAcrossEntities: { searchResults: [{ entity: CHILD_TERM }] } },
        });
        mockUseApolloClient.mockReturnValue({ query });

        const { result } = renderHook(() => useGlossaryTreeEntities());
        await act(async () => {
            result.current.expandNode(ROOT_NODE.urn);
        });
        await act(async () => {
            await flushPromises();
        });
        expect(result.current.entities.some((e) => e.urn === CHILD_TERM.urn)).toBe(true);

        act(() => {
            result.current.collapseNode(ROOT_NODE.urn);
        });
        expect(result.current.entities.some((e) => e.urn === CHILD_TERM.urn)).toBe(false);
        expect(result.current.expandedNodes.has(ROOT_NODE.urn)).toBe(false);
    });

    it('builds a multi-level parent chain when expanding a child node', async () => {
        // First call returns the root's child node; second call (when child is expanded) returns
        // a grandchild term. The grandchild's synthesized parentNodes should be [child, root].
        const grandchildTerm = {
            urn: 'urn:li:glossaryTerm:grandchild',
            type: EntityType.GlossaryTerm,
            properties: { name: 'Grandchild' },
        };
        const query = vi
            .fn()
            .mockResolvedValueOnce({
                data: { scrollAcrossEntities: { searchResults: [{ entity: CHILD_NODE }] } },
            })
            .mockResolvedValueOnce({
                data: { scrollAcrossEntities: { searchResults: [{ entity: grandchildTerm }] } },
            });
        mockUseApolloClient.mockReturnValue({ query });

        const { result } = renderHook(() => useGlossaryTreeEntities());
        await act(async () => {
            result.current.expandNode(ROOT_NODE.urn);
        });
        await act(async () => {
            await flushPromises();
        });

        await act(async () => {
            result.current.expandNode(CHILD_NODE.urn);
        });
        await act(async () => {
            await flushPromises();
        });

        const grandchild = result.current.entities.find((e) => e.urn === grandchildTerm.urn);
        expect(grandchild).toBeDefined();
        const chain = (grandchild as any).parentNodes.nodes.map((n: any) => n.urn);
        // Direct-parent-first: grandchild's chain is [child, root].
        expect(chain).toEqual([CHILD_NODE.urn, ROOT_NODE.urn]);
    });
});
