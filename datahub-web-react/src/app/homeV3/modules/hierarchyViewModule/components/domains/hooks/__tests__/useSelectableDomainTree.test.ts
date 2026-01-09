import { ApolloQueryResult, NetworkStatus } from '@apollo/client';
import { act } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useInitialDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useInitialDomains';
import useLoadMoreRootDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useLoadMoreRootDomains';
import useRootDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useRootDomains';
import useSelectableDomainTree from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useSelectableDomainTree';
import useTreeNodesFromFlatDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromFlatDomains';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromListDomains';
import { convertDomainToTreeNode } from '@app/homeV3/modules/hierarchyViewModule/components/domains/utils';
import useTree from '@app/homeV3/modules/hierarchyViewModule/treeView/useTree';
import { mergeTrees } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

import { GetSearchResultsForMultipleQuery } from '@graphql/search.generated';
import { Domain, EntityType } from '@types';

// Mock all the dependent hooks
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useInitialDomains');
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useRootDomains');
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useLoadMoreRootDomains');
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromFlatDomains');
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromListDomains');
vi.mock('@app/homeV3/modules/hierarchyViewModule/treeView/useTree');
vi.mock('@app/homeV3/modules/hierarchyViewModule/treeView/utils');
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/domains/utils', () => ({
    convertDomainToTreeNode: vi.fn((domain, isRoot) => ({
        value: domain.urn,
        label: domain.properties?.name,
        entity: domain,
        isRoot,
    })),
}));

const mockUseInitialDomains = vi.mocked(useInitialDomains);
const mockUseRootDomains = vi.mocked(useRootDomains);
const mockUseLoadMoreRootDomains = vi.mocked(useLoadMoreRootDomains);
const mockUseTreeNodesFromFlatDomains = vi.mocked(useTreeNodesFromFlatDomains);
const mockUseTreeNodesFromDomains = vi.mocked(useTreeNodesFromDomains);
const mockUseTree = vi.mocked(useTree);
const mockMergeTrees = vi.mocked(mergeTrees);
const mockConvertDomainToTreeNode = vi.mocked(convertDomainToTreeNode);

const mockRefetch = (): Promise<ApolloQueryResult<GetSearchResultsForMultipleQuery>> => {
    return new Promise<ApolloQueryResult<GetSearchResultsForMultipleQuery>>((resolve) => {
        resolve({ data: { searchAcrossEntities: undefined }, networkStatus: NetworkStatus.refetch, loading: false });
    });
};

// Mock tree object with dynamic nodes
let mockTreeNodesState: any[] = [];
const mockTreeMethods = {
    get nodes() {
        return mockTreeNodesState;
    },
    replace: vi.fn((newNodes: any[]) => {
        mockTreeNodesState = newNodes;
    }),
    merge: vi.fn((newNodes: any[]) => {
        mockTreeNodesState = [...mockTreeNodesState, ...newNodes];
    }),
    update: vi.fn(),
    updateNode: vi.fn(),
};

// Mock domain entities
const mockDomain1 = {
    urn: 'urn:li:domain:1',
    id: 'domain-1',
    type: EntityType.Domain,
    name: 'Domain 1',
    properties: {
        name: 'Domain 1',
    },
};

const mockDomain2 = {
    urn: 'urn:li:domain:2',
    id: 'domain-2',
    type: EntityType.Domain,
    name: 'Domain 2',
    properties: {
        name: 'Domain 2',
    },
};

const mockDomain3 = {
    urn: 'urn:li:domain:3',
    id: 'domain-3',
    type: EntityType.Domain,
    name: 'Domain 3',
    properties: {
        name: 'Domain 3',
    },
};

// Mock tree nodes
const mockTreeNode1 = {
    value: 'urn:li:domain:1',
    label: 'Domain 1',
    entity: mockDomain1,
};

const mockTreeNode2 = {
    value: 'urn:li:domain:2',
    label: 'Domain 2',
    entity: mockDomain2,
    isRoot: true,
};

const mockTreeNode3 = {
    value: 'urn:li:domain:3',
    label: 'Domain 3',
    entity: mockDomain3,
    isRoot: true,
};

describe('useSelectableDomainTree hook', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        // Reset mock tree nodes
        mockTreeNodesState = [];

        // Setup default mock implementations
        mockUseInitialDomains.mockReturnValue({ data: undefined, domains: [], loading: false });
        mockUseRootDomains.mockReturnValue({
            data: undefined,
            domains: [],
            total: 0,
            loading: false,
            refetch: mockRefetch,
        });
        mockUseLoadMoreRootDomains.mockReturnValue({
            loading: false,
            loadMoreRootDomains: () =>
                new Promise<Domain[]>((resolve) => {
                    resolve([]);
                }),
        });
        mockUseTreeNodesFromFlatDomains.mockReturnValue(undefined);
        mockUseTreeNodesFromDomains.mockReturnValue(undefined);
        mockUseTree.mockReturnValue(mockTreeMethods);
        mockMergeTrees.mockReturnValue([]);
    });

    describe('initialization', () => {
        it('should initialize with empty selected values when no initial domains provided', () => {
            const { result } = renderHook(() => useSelectableDomainTree(undefined));

            expect(result.current.selectedValues).toEqual([]);
            expect(result.current.loading).toBe(true);
        });

        it('should initialize with provided selected domain URNs', () => {
            const initialUrns = ['urn:li:domain:1', 'urn:li:domain:2'];
            const { result } = renderHook(() => useSelectableDomainTree(initialUrns));

            expect(result.current.selectedValues).toEqual(initialUrns);
            expect(mockUseInitialDomains).toHaveBeenCalledWith(initialUrns);
        });

        it('should initialize with URNs from props when props are provided', () => {
            const initialUrns = ['urn:li:domain:1'];

            renderHook(() => useSelectableDomainTree(initialUrns));

            expect(mockUseInitialDomains).toHaveBeenCalledWith(initialUrns);
        });

        it('should initialize with empty URNs when props are not provided', () => {
            renderHook(() => useSelectableDomainTree(undefined));

            expect(mockUseInitialDomains).toHaveBeenCalledWith([]);
        });
    });

    describe('tree initialization', () => {
        it('should not initialize tree when root domains are loading', () => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [],
                total: 0,
                loading: true,
                refetch: mockRefetch,
            });

            renderHook(() => useSelectableDomainTree([]));

            expect(mockTreeMethods.replace).not.toHaveBeenCalled();
        });

        it('should not initialize tree when root tree nodes are undefined', () => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue(undefined);

            renderHook(() => useSelectableDomainTree([]));

            expect(mockTreeMethods.replace).not.toHaveBeenCalled();
        });

        it('should not initialize tree when initial selected tree nodes are undefined', () => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue(undefined);

            renderHook(() => useSelectableDomainTree([]));

            expect(mockTreeMethods.replace).not.toHaveBeenCalled();
        });

        it('should initialize tree when all dependencies are ready', () => {
            const rootTreeNodes = [mockTreeNode1];
            const initialSelectedTreeNodes = [mockTreeNode2];
            const mergedTreeNodes = [mockTreeNode1, mockTreeNode2];

            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue(rootTreeNodes);
            mockUseTreeNodesFromFlatDomains.mockReturnValue(initialSelectedTreeNodes);
            mockMergeTrees.mockReturnValue(mergedTreeNodes);

            renderHook(() => useSelectableDomainTree([]));

            expect(mockMergeTrees).toHaveBeenCalledWith(rootTreeNodes, []);
            expect(mockTreeMethods.replace).toHaveBeenCalledWith(mergedTreeNodes);
        });
    });

    describe('selectedValues management', () => {
        beforeEach(() => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue([mockTreeNode2]);
        });

        it('should return tree nodes from merge result', () => {
            const mergedTreeNodes = [mockTreeNode1, mockTreeNode2];
            mockMergeTrees.mockReturnValue(mergedTreeNodes);

            const { result } = renderHook(() => useSelectableDomainTree([]));

            expect(result.current.tree.nodes).toEqual(mergedTreeNodes);
        });

        it('should update selected values when setSelectedValues is called', () => {
            const { result } = renderHook(() => useSelectableDomainTree([]));

            act(() => {
                result.current.setSelectedValues(['urn:li:domain:3']);
            });

            expect(result.current.selectedValues).toEqual(['urn:li:domain:3']);
        });

        it('should handle loading state correctly when root domains are loading', () => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [],
                total: 0,
                loading: true,
                refetch: mockRefetch,
            });

            const { result } = renderHook(() => useSelectableDomainTree([]));

            expect(result.current.loading).toBe(true);
        });

        it('should handle ready state when all data is loaded', () => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);

            const { result } = renderHook(() => useSelectableDomainTree([]));

            expect(result.current.loading).toBe(false);
        });
    });

    describe('tree method delegation', () => {
        beforeEach(() => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue([mockTreeNode2]);
        });

        it('should delegate merge calls to underlying tree', () => {
            const nodesToMerge = [mockTreeNode2];
            const { result } = renderHook(() => useSelectableDomainTree([]));

            result.current.tree.merge(nodesToMerge);

            expect(mockTreeMethods.merge).toHaveBeenCalledWith(nodesToMerge);
        });

        it('should delegate update calls to underlying tree', () => {
            const nodesToUpdate = [mockTreeNode2];
            const parentValue = 'parent';
            const { result } = renderHook(() => useSelectableDomainTree([]));

            result.current.tree.update(nodesToUpdate, parentValue);

            expect(mockTreeMethods.update).toHaveBeenCalledWith(nodesToUpdate, parentValue);
        });

        it('should delegate updateNode calls to underlying tree', () => {
            const value = 'test-value';
            const changes = { label: 'Updated Label' };
            const { result } = renderHook(() => useSelectableDomainTree([]));

            result.current.tree.updateNode(value, changes);

            expect(mockTreeMethods.updateNode).toHaveBeenCalledWith(value, changes);
        });
    });

    describe('integration with dependent hooks', () => {
        it('should use flattened domains from initial domains', () => {
            const initialDomains = [mockDomain1, mockDomain2];
            const initialUrns = ['urn:li:domain:1', 'urn:li:domain:2'];

            mockUseInitialDomains.mockReturnValue({ data: undefined, domains: initialDomains, loading: false });

            renderHook(() => useSelectableDomainTree(initialUrns));

            expect(mockUseTreeNodesFromFlatDomains).toHaveBeenCalledWith(initialDomains);
        });

        it('should use domains from root domains hook', () => {
            const rootDomains = [mockDomain1];

            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: rootDomains,
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });

            renderHook(() => useSelectableDomainTree([]));

            expect(mockUseTreeNodesFromDomains).toHaveBeenCalledWith(rootDomains, false);
        });

        it('should handle mixed states correctly', () => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue([mockTreeNode2]);

            const { result } = renderHook(() => useSelectableDomainTree([]));

            expect(result.current.loading).toBe(false);
            expect(mockMergeTrees).toHaveBeenCalledWith([mockTreeNode1], []);
        });

        it('should handle empty initial domains', () => {
            mockUseInitialDomains.mockReturnValue({ data: undefined, domains: [], loading: false });

            renderHook(() => useSelectableDomainTree([]));

            expect(mockUseTreeNodesFromFlatDomains).toHaveBeenCalledWith([]);
        });

        it('should handle empty root domains', () => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue([mockTreeNode2]);

            renderHook(() => useSelectableDomainTree([]));

            expect(mockUseTreeNodesFromDomains).toHaveBeenCalledWith([mockDomain1], false);
        });
    });

    describe('loadMoreRootNodes', () => {
        let mockLoadMore: ReturnType<typeof vi.fn>;

        beforeEach(() => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue([]);
            mockLoadMore = vi.fn(); // Initialize as a simple spy
            mockUseLoadMoreRootDomains.mockReturnValue({
                loading: false,
                loadMoreRootDomains: mockLoadMore,
            });
            mockMergeTrees.mockImplementation((existing, newNodes) => [...existing, ...newNodes]);
            mockConvertDomainToTreeNode.mockImplementation((domain, isRoot) => ({
                value: domain.urn,
                label: domain.properties?.name || '', // Provide a fallback for label
                entity: domain,
                isRoot,
            }));
        });

        it('should not load more root domains if already loading', async () => {
            const localMockLoadMore = vi.fn();
            mockUseLoadMoreRootDomains.mockReturnValue({
                loading: true,
                loadMoreRootDomains: localMockLoadMore,
            });

            const { result } = renderHook(() => useSelectableDomainTree([]));

            await act(async () => {
                await result.current.loadMoreRootNodes();
            });

            expect(localMockLoadMore).not.toHaveBeenCalled();
        });

        it('should not load more root domains if no more root nodes exist', async () => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 1,
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue([]);
            const localMockLoadMore = vi.fn();
            mockUseLoadMoreRootDomains.mockReturnValue({
                loading: false,
                loadMoreRootDomains: localMockLoadMore,
            });

            const { result } = renderHook(() => useSelectableDomainTree([]));

            act(() => {
                // Manually set finalRootDomainsTotal to match tree.nodes.length
                // to simulate no more root nodes
                result.current.rootNodesTotal = result.current.tree.nodes.length;
            });

            await act(async () => {
                await result.current.loadMoreRootNodes();
            });

            expect(localMockLoadMore).not.toHaveBeenCalled();
        });

        it('should load more root domains and merge them into the tree', async () => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 2, // Indicate there's more to load
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue([]);
            mockLoadMore.mockImplementationOnce(
                (_skip: number, _limit: number) =>
                    new Promise<Domain[]>((resolve) => {
                        resolve([mockDomain2]);
                    }),
            );
            mockUseLoadMoreRootDomains.mockReturnValue({
                loading: false,
                loadMoreRootDomains: mockLoadMore,
            });

            const { result } = renderHook(() => useSelectableDomainTree([]));

            expect(result.current.tree.nodes).toEqual([mockTreeNode1]);

            await act(async () => {
                await result.current.loadMoreRootNodes();
            });

            expect(mockLoadMore).toHaveBeenCalledWith(1, 5); // 1 existing node, default batch size
            expect(mockConvertDomainToTreeNode).toHaveBeenCalledWith(mockDomain2, true);
            expect(mockTreeMethods.merge).toHaveBeenCalledWith([mockTreeNode2]);
            expect(result.current.tree.nodes).toEqual([mockTreeNode1, mockTreeNode2]);
        });

        it('should set finalRootDomainsTotal to current tree length if no new domains are fetched', async () => {
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 2, // Indicate there's more to load initially
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue([]);
            mockLoadMore.mockImplementationOnce(
                (_skip: number, _limit: number) =>
                    new Promise<Domain[]>((resolve) => {
                        resolve([]); // No new domains
                    }),
            );
            mockUseLoadMoreRootDomains.mockReturnValue({
                loading: false,
                loadMoreRootDomains: mockLoadMore,
            });

            const { result } = renderHook(() => useSelectableDomainTree([]));

            expect(result.current.rootNodesTotal).toBe(2);

            await act(async () => {
                await result.current.loadMoreRootNodes();
            });

            expect(mockLoadMore).toHaveBeenCalledWith(1, 5);
            expect(result.current.rootNodesTotal).toBe(result.current.tree.nodes.length);
            expect(result.current.rootNodesTotal).toBe(1); // Only mockTreeNode1 is in the tree
        });

        it('should preprocess root nodes before merging', async () => {
            const initialSelectedTreeNodes = [{ ...mockTreeNode2, selected: true }];
            mockUseInitialDomains.mockReturnValue({ data: undefined, domains: [mockDomain2], loading: false });
            mockUseTreeNodesFromFlatDomains.mockReturnValue(initialSelectedTreeNodes);

            // Set total to be greater than initial nodes to trigger loadMoreRootDomains
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1, mockDomain2],
                total: 3, // Changed to 3 to ensure loadMoreRootDomains is called
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1, mockTreeNode2]);
            mockMergeTrees.mockImplementation((existing, newNodes) => {
                // Simulate mergeTrees logic for preprocessRootNodes
                const merged = [...existing];
                newNodes.forEach((newNode) => {
                    const existingNodeIndex = merged.findIndex((node) => node.value === newNode.value);
                    if (existingNodeIndex > -1) {
                        merged[existingNodeIndex] = { ...merged[existingNodeIndex], ...newNode };
                    } else {
                        merged.push(newNode);
                    }
                });
                return merged;
            });

            const { result } = renderHook(() => useSelectableDomainTree([]));

            expect(mockMergeTrees).toHaveBeenCalledWith(
                [mockTreeNode1, mockTreeNode2],
                [{ ...mockTreeNode2, selected: true }],
            );
            expect(result.current.tree.nodes).toEqual([mockTreeNode1, { ...mockTreeNode2, selected: true }]);

            // Now test loadMoreRootNodes with preprocessing
            mockUseRootDomains.mockReturnValue({
                data: undefined,
                domains: [mockDomain1],
                total: 3, // More to load
                loading: false,
                refetch: mockRefetch,
            });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]); // Initial root nodes
            mockLoadMore.mockImplementationOnce(
                (_skip: number, _limit: number) =>
                    new Promise<Domain[]>((resolve) => {
                        resolve([mockDomain3]); // Load mockDomain3
                    }),
            );
            mockUseLoadMoreRootDomains.mockReturnValue({
                loading: false,
                loadMoreRootDomains: mockLoadMore,
            });

            await act(async () => {
                await result.current.loadMoreRootNodes();
            });

            // Expect mockDomain3 to be converted and merged, with preprocessing applied (though no overlap with initialSelectedTreeNodes here)
            expect(mockLoadMore).toHaveBeenCalledWith(2, 5);
            expect(mockConvertDomainToTreeNode).toHaveBeenCalledWith(mockDomain3, true);
            expect(mockTreeMethods.merge).toHaveBeenCalledWith([mockTreeNode3]);
            expect(result.current.tree.nodes).toEqual([
                mockTreeNode1,
                { ...mockTreeNode2, selected: true },
                mockTreeNode3,
            ]);
        });
    });
});
