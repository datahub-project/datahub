import { act } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useInitialDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useInitialDomains';
import useRootDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useRootDomains';
import useSelectableDomainTree from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useSelectableDomainTree';
import useTreeNodesFromFlatDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromFlatDomains';
import useTreeNodesFromDomains from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromListDomains';
import useTree from '@app/homeV3/modules/hierarchyViewModule/treeView/useTree';
import { mergeTrees } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

import { EntityType } from '@types';

// Mock all the dependent hooks
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useInitialDomains');
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useRootDomains');
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromFlatDomains');
vi.mock('@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useTreeNodesFromListDomains');
vi.mock('@app/homeV3/modules/hierarchyViewModule/treeView/useTree');
vi.mock('@app/homeV3/modules/hierarchyViewModule/treeView/utils');

const mockUseInitialDomains = vi.mocked(useInitialDomains);
const mockUseRootDomains = vi.mocked(useRootDomains);
const mockUseTreeNodesFromFlatDomains = vi.mocked(useTreeNodesFromFlatDomains);
const mockUseTreeNodesFromDomains = vi.mocked(useTreeNodesFromDomains);
const mockUseTree = vi.mocked(useTree);
const mockMergeTrees = vi.mocked(mergeTrees);

// Mock tree object with dynamic nodes
const mockTreeMethods = {
    nodes: [] as any[],
    replace: vi.fn((newNodes: any[]) => {
        mockTreeMethods.nodes = newNodes;
    }),
    merge: vi.fn(),
    update: vi.fn(),
    updateNode: vi.fn(),
};

// Mock domain entities
const mockDomain1 = {
    urn: 'urn:li:domain:1',
    id: 'domain-1',
    type: EntityType.Domain,
    name: 'Domain 1',
};

const mockDomain2 = {
    urn: 'urn:li:domain:2',
    id: 'domain-2',
    type: EntityType.Domain,
    name: 'Domain 2',
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
};

describe('useSelectableDomainTree hook', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        // Reset mock tree nodes
        mockTreeMethods.nodes = [];

        // Setup default mock implementations
        mockUseInitialDomains.mockReturnValue({ data: undefined, domains: [], loading: false });
        mockUseRootDomains.mockReturnValue({ data: undefined, domains: [], total: 0, loading: false });
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
            mockUseRootDomains.mockReturnValue({ data: undefined, domains: [], total: 0, loading: true });

            renderHook(() => useSelectableDomainTree([]));

            expect(mockTreeMethods.replace).not.toHaveBeenCalled();
        });

        it('should not initialize tree when root tree nodes are undefined', () => {
            mockUseRootDomains.mockReturnValue({ data: undefined, domains: [mockDomain1], total: 1, loading: false });
            mockUseTreeNodesFromDomains.mockReturnValue(undefined);

            renderHook(() => useSelectableDomainTree([]));

            expect(mockTreeMethods.replace).not.toHaveBeenCalled();
        });

        it('should not initialize tree when initial selected tree nodes are undefined', () => {
            mockUseRootDomains.mockReturnValue({ data: undefined, domains: [mockDomain1], total: 1, loading: false });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue(undefined);

            renderHook(() => useSelectableDomainTree([]));

            expect(mockTreeMethods.replace).not.toHaveBeenCalled();
        });

        it('should initialize tree when all dependencies are ready', () => {
            const rootTreeNodes = [mockTreeNode1];
            const initialSelectedTreeNodes = [mockTreeNode2];
            const mergedTreeNodes = [mockTreeNode1, mockTreeNode2];

            mockUseRootDomains.mockReturnValue({ data: undefined, domains: [mockDomain1], total: 1, loading: false });
            mockUseTreeNodesFromDomains.mockReturnValue(rootTreeNodes);
            mockUseTreeNodesFromFlatDomains.mockReturnValue(initialSelectedTreeNodes);
            mockMergeTrees.mockReturnValue(mergedTreeNodes);

            renderHook(() => useSelectableDomainTree([]));

            expect(mockMergeTrees).toHaveBeenCalledWith(rootTreeNodes, initialSelectedTreeNodes);
            expect(mockTreeMethods.replace).toHaveBeenCalledWith(mergedTreeNodes);
        });
    });

    describe('selectedValues management', () => {
        beforeEach(() => {
            mockUseRootDomains.mockReturnValue({ data: undefined, domains: [mockDomain1], total: 1, loading: false });
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
            mockUseRootDomains.mockReturnValue({ data: undefined, domains: [], total: 0, loading: true });

            const { result } = renderHook(() => useSelectableDomainTree([]));

            expect(result.current.loading).toBe(true);
        });

        it('should handle ready state when all data is loaded', () => {
            mockUseRootDomains.mockReturnValue({ data: undefined, domains: [mockDomain1], total: 1, loading: false });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);

            const { result } = renderHook(() => useSelectableDomainTree([]));

            expect(result.current.loading).toBe(false);
        });
    });

    describe('tree method delegation', () => {
        beforeEach(() => {
            mockUseRootDomains.mockReturnValue({ data: undefined, domains: [mockDomain1], total: 1, loading: false });
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

            mockUseRootDomains.mockReturnValue({ data: undefined, domains: rootDomains, total: 1, loading: false });

            renderHook(() => useSelectableDomainTree([]));

            expect(mockUseTreeNodesFromDomains).toHaveBeenCalledWith(rootDomains);
        });

        it('should handle mixed states correctly', () => {
            mockUseRootDomains.mockReturnValue({ data: undefined, domains: [mockDomain1], total: 1, loading: false });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue([mockTreeNode2]);

            const { result } = renderHook(() => useSelectableDomainTree([]));

            expect(result.current.loading).toBe(false);
            expect(mockMergeTrees).toHaveBeenCalledWith([mockTreeNode1], [mockTreeNode2]);
        });

        it('should handle empty initial domains', () => {
            mockUseInitialDomains.mockReturnValue({ data: undefined, domains: [], loading: false });

            renderHook(() => useSelectableDomainTree([]));

            expect(mockUseTreeNodesFromFlatDomains).toHaveBeenCalledWith([]);
        });

        it('should handle empty root domains', () => {
            mockUseRootDomains.mockReturnValue({ data: undefined, domains: [mockDomain1], total: 1, loading: false });
            mockUseTreeNodesFromDomains.mockReturnValue([mockTreeNode1]);
            mockUseTreeNodesFromFlatDomains.mockReturnValue([mockTreeNode2]);

            renderHook(() => useSelectableDomainTree([]));

            expect(mockUseTreeNodesFromDomains).toHaveBeenCalledWith([mockDomain1]);
        });
    });
});
