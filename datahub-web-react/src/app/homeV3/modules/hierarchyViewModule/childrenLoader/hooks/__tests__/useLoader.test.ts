import { renderHook } from '@testing-library/react-hooks';
import { type MockedFunction, beforeEach, describe, expect, it, vi } from 'vitest';

import useLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useLoader';
import {
    ChildrenLoaderMetadata,
    ChildrenLoaderType,
} from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { EntityType } from '@types';

// Mock the context hook
const mockGet = vi.fn();
const mockOnLoad = vi.fn();
const mockMaxNumberOfChildrenToLoad = 10;

vi.mock('@app/homeV3/modules/hierarchyViewModule/childrenLoader/context/useChildrenLoaderContext', () => ({
    useChildrenLoaderContext: () => ({
        get: mockGet,
        onLoad: mockOnLoad,
        maxNumberOfChildrenToLoad: mockMaxNumberOfChildrenToLoad,
    }),
}));

// Helper function to create tree nodes
function createTreeNode(value: string, parentValue?: string): TreeNode {
    return {
        value,
        label: value,
        entity: { urn: value, type: EntityType.Domain },
        parentValue,
    };
}

describe('useLoader hook', () => {
    const mockParentValue = 'urn:li:domain:parent';
    let mockLoadChildren: MockedFunction<ChildrenLoaderType>;
    let mockLoadRelatedEntities: MockedFunction<ChildrenLoaderType>;

    beforeEach(() => {
        vi.clearAllMocks();

        // Default mock implementations
        mockGet.mockReturnValue(undefined);

        mockLoadChildren = vi.fn().mockReturnValue({
            nodes: [],
            loading: false,
            total: 0,
        });

        mockLoadRelatedEntities = vi.fn().mockReturnValue({
            nodes: [],
            loading: false,
            total: 0,
        });
    });

    describe('context usage', () => {
        it('should get metadata from context using parentValue', () => {
            const mockMetadata: ChildrenLoaderMetadata = {
                numberOfLoadedChildren: 5,
                totalNumberOfChildren: 10,
            };
            mockGet.mockReturnValue(mockMetadata);

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, undefined));

            expect(mockGet).toHaveBeenCalledWith(mockParentValue);
        });

        it('should use maxNumberOfChildrenToLoad from context', () => {
            renderHook(() => useLoader(mockParentValue, mockLoadChildren, undefined));

            expect(mockLoadChildren).toHaveBeenCalledWith({
                parentValue: mockParentValue,
                metadata: undefined,
                maxNumberToLoad: mockMaxNumberOfChildrenToLoad,
            });
        });
    });

    describe('children loading', () => {
        it('should call loadChildren with correct parameters', () => {
            const mockMetadata: ChildrenLoaderMetadata = {
                numberOfLoadedChildren: 3,
                totalNumberOfChildren: 8,
            };
            mockGet.mockReturnValue(mockMetadata);

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, undefined));

            expect(mockLoadChildren).toHaveBeenCalledWith({
                parentValue: mockParentValue,
                metadata: mockMetadata,
                maxNumberToLoad: mockMaxNumberOfChildrenToLoad,
            });
        });

        it('should handle children loading response correctly', () => {
            const mockChildrenNodes = [createTreeNode('child1'), createTreeNode('child2')];
            mockLoadChildren.mockReturnValue({
                nodes: mockChildrenNodes,
                loading: false,
                total: 5,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, undefined));

            expect(mockOnLoad).toHaveBeenCalledWith(
                mockChildrenNodes,
                {
                    numberOfLoadedChildren: 2,
                    numberOfLoadedRelatedEntities: 0,
                    totalNumberOfChildren: 5,
                    totalNumberOfRelatedEntities: 0,
                },
                mockParentValue,
            );
        });
    });

    describe('related entities loading', () => {
        it('should not call loadRelatedEntities when undefined', () => {
            renderHook(() => useLoader(mockParentValue, mockLoadChildren, undefined));

            expect(mockLoadRelatedEntities).not.toHaveBeenCalled();
        });

        it('should call loadRelatedEntities with correct parameters', () => {
            const mockMetadata: ChildrenLoaderMetadata = {
                numberOfLoadedRelatedEntities: 2,
                totalNumberOfRelatedEntities: 6,
            };
            mockGet.mockReturnValue(mockMetadata);

            const mockChildrenNodes = [createTreeNode('child1'), createTreeNode('child2')];
            mockLoadChildren.mockReturnValue({
                nodes: mockChildrenNodes,
                loading: true, // children still loading
                total: 5,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, mockLoadRelatedEntities));

            expect(mockLoadRelatedEntities).toHaveBeenCalledWith({
                parentValue: mockParentValue,
                metadata: mockMetadata,
                dependenciesIsLoading: true, // children loading state
                maxNumberToLoad: 8, // 10 - 2 children nodes
            });
        });

        it('should calculate correct maxNumberToLoad for related entities', () => {
            const mockChildrenNodes = [createTreeNode('child1'), createTreeNode('child2'), createTreeNode('child3')];
            mockLoadChildren.mockReturnValue({
                nodes: mockChildrenNodes,
                loading: false,
                total: 5,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, mockLoadRelatedEntities));

            expect(mockLoadRelatedEntities).toHaveBeenCalledWith({
                parentValue: mockParentValue,
                metadata: undefined,
                dependenciesIsLoading: false,
                maxNumberToLoad: 7, // 10 - 3 children nodes
            });
        });

        it('should handle undefined children nodes in maxNumberToLoad calculation', () => {
            mockLoadChildren.mockReturnValue({
                nodes: undefined,
                loading: false,
                total: 5,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, mockLoadRelatedEntities));

            expect(mockLoadRelatedEntities).toHaveBeenCalledWith({
                parentValue: mockParentValue,
                metadata: undefined,
                dependenciesIsLoading: false,
                maxNumberToLoad: 10, // full amount when no children nodes
            });
        });
    });

    describe('loading coordination', () => {
        it('should not call onLoad while children are loading', () => {
            mockLoadChildren.mockReturnValue({
                nodes: [createTreeNode('child1')],
                loading: true, // still loading
                total: 2,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, undefined));

            expect(mockOnLoad).not.toHaveBeenCalled();
        });

        it('should not call onLoad while related entities are loading', () => {
            mockLoadChildren.mockReturnValue({
                nodes: [createTreeNode('child1')],
                loading: false,
                total: 2,
            });

            mockLoadRelatedEntities.mockReturnValue({
                nodes: [createTreeNode('related1')],
                loading: true, // still loading
                total: 3,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, mockLoadRelatedEntities));

            expect(mockOnLoad).not.toHaveBeenCalled();
        });

        it('should call onLoad when both children and related entities finish loading', () => {
            const mockChildrenNodes = [createTreeNode('child1')];
            const mockRelatedNodes = [createTreeNode('related1')];

            mockLoadChildren.mockReturnValue({
                nodes: mockChildrenNodes,
                loading: false,
                total: 5,
            });

            mockLoadRelatedEntities.mockReturnValue({
                nodes: mockRelatedNodes,
                loading: false,
                total: 3,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, mockLoadRelatedEntities));

            expect(mockOnLoad).toHaveBeenCalledWith(
                [...mockChildrenNodes, ...mockRelatedNodes],
                {
                    numberOfLoadedChildren: 1,
                    numberOfLoadedRelatedEntities: 1,
                    totalNumberOfChildren: 5,
                    totalNumberOfRelatedEntities: 3,
                },
                mockParentValue,
            );
        });
    });

    describe('metadata calculation', () => {
        it('should combine existing metadata with new counts', () => {
            const existingMetadata: ChildrenLoaderMetadata = {
                numberOfLoadedChildren: 3,
                numberOfLoadedRelatedEntities: 2,
                totalNumberOfChildren: 10,
                totalNumberOfRelatedEntities: 8,
            };
            mockGet.mockReturnValue(existingMetadata);

            const mockChildrenNodes = [createTreeNode('child1'), createTreeNode('child2')];
            const mockRelatedNodes = [createTreeNode('related1')];

            mockLoadChildren.mockReturnValue({
                nodes: mockChildrenNodes,
                loading: false,
                total: 12, // new total
            });

            mockLoadRelatedEntities.mockReturnValue({
                nodes: mockRelatedNodes,
                loading: false,
                total: 9, // new total
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, mockLoadRelatedEntities));

            expect(mockOnLoad).toHaveBeenCalledWith(
                [...mockChildrenNodes, ...mockRelatedNodes],
                {
                    numberOfLoadedChildren: 5, // 3 + 2
                    numberOfLoadedRelatedEntities: 3, // 2 + 1
                    totalNumberOfChildren: 12, // updated
                    totalNumberOfRelatedEntities: 9, // updated
                },
                mockParentValue,
            );
        });

        it('should handle undefined existing metadata', () => {
            mockGet.mockReturnValue(undefined);

            const mockChildrenNodes = [createTreeNode('child1')];
            const mockRelatedNodes = [createTreeNode('related1')];

            mockLoadChildren.mockReturnValue({
                nodes: mockChildrenNodes,
                loading: false,
                total: 5,
            });

            mockLoadRelatedEntities.mockReturnValue({
                nodes: mockRelatedNodes,
                loading: false,
                total: 3,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, mockLoadRelatedEntities));

            expect(mockOnLoad).toHaveBeenCalledWith(
                [...mockChildrenNodes, ...mockRelatedNodes],
                {
                    numberOfLoadedChildren: 1, // 0 + 1
                    numberOfLoadedRelatedEntities: 1, // 0 + 1
                    totalNumberOfChildren: 5,
                    totalNumberOfRelatedEntities: 3,
                },
                mockParentValue,
            );
        });

        it('should not include totals in metadata when undefined', () => {
            const mockChildrenNodes = [createTreeNode('child1')];

            mockLoadChildren.mockReturnValue({
                nodes: mockChildrenNodes,
                loading: false,
                total: undefined, // no total provided
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, undefined));

            expect(mockOnLoad).toHaveBeenCalledWith(
                mockChildrenNodes,
                {
                    numberOfLoadedChildren: 1,
                    numberOfLoadedRelatedEntities: 0,
                    totalNumberOfRelatedEntities: 0,
                    // totalNumberOfChildren should not be set
                },
                mockParentValue,
            );
        });
    });

    describe('node combination', () => {
        it('should combine children and related entity nodes correctly', () => {
            const mockChildrenNodes = [createTreeNode('child1'), createTreeNode('child2')];
            const mockRelatedNodes = [createTreeNode('related1'), createTreeNode('related2')];

            mockLoadChildren.mockReturnValue({
                nodes: mockChildrenNodes,
                loading: false,
                total: 2,
            });

            mockLoadRelatedEntities.mockReturnValue({
                nodes: mockRelatedNodes,
                loading: false,
                total: 2,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, mockLoadRelatedEntities));

            expect(mockOnLoad).toHaveBeenCalledWith(
                [...mockChildrenNodes, ...mockRelatedNodes],
                expect.any(Object),
                mockParentValue,
            );
        });

        it('should handle undefined nodes arrays', () => {
            mockLoadChildren.mockReturnValue({
                nodes: undefined,
                loading: false,
                total: 0,
            });

            mockLoadRelatedEntities.mockReturnValue({
                nodes: undefined,
                loading: false,
                total: 0,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, mockLoadRelatedEntities));

            expect(mockOnLoad).toHaveBeenCalledWith(
                [], // should default to empty array
                expect.any(Object),
                mockParentValue,
            );
        });
    });

    describe('edge cases', () => {
        it('should handle children loading with zero nodes', () => {
            mockLoadChildren.mockReturnValue({
                nodes: [],
                loading: false,
                total: 0,
            });

            renderHook(() => useLoader(mockParentValue, mockLoadChildren, undefined));

            expect(mockOnLoad).toHaveBeenCalledWith(
                [],
                {
                    numberOfLoadedChildren: 0,
                    numberOfLoadedRelatedEntities: 0,
                    totalNumberOfChildren: 0,
                    totalNumberOfRelatedEntities: 0,
                },
                mockParentValue,
            );
        });

        it('should re-trigger effect when dependencies change', () => {
            const { rerender } = renderHook(({ parentValue }) => useLoader(parentValue, mockLoadChildren, undefined), {
                initialProps: { parentValue: mockParentValue },
            });

            mockLoadChildren.mockReturnValue({
                nodes: [createTreeNode('child1')],
                loading: false,
                total: 1,
            });

            // First call
            expect(mockOnLoad).toHaveBeenCalledTimes(1);

            // Change parent value and rerender
            const newParentValue = 'urn:li:domain:newparent';
            rerender({ parentValue: newParentValue });

            // Should trigger effect again
            expect(mockOnLoad).toHaveBeenCalledTimes(2);
            expect(mockOnLoad).toHaveBeenLastCalledWith(expect.any(Array), expect.any(Object), newParentValue);
        });

        it('should handle metadata update during loading state changes', () => {
            // Start with loading state
            mockLoadChildren.mockReturnValue({
                nodes: [createTreeNode('child1')],
                loading: true,
                total: 1,
            });

            const { rerender } = renderHook(() => useLoader(mockParentValue, mockLoadChildren, undefined));

            // Should not call onLoad yet
            expect(mockOnLoad).not.toHaveBeenCalled();

            // Update to finished loading
            mockLoadChildren.mockReturnValue({
                nodes: [createTreeNode('child1')],
                loading: false,
                total: 1,
            });

            rerender();

            // Now should call onLoad
            expect(mockOnLoad).toHaveBeenCalledTimes(1);
        });
    });
});
