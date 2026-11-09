import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useGlossaryChildren from '@app/entityV2/glossaryNode/useGlossaryChildren';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { useGetGlossaryNodeCanManageChildrenQuery } from '@src/graphql/lifecycle.generated';
import { useGetAutoCompleteMultipleResultsQuery, useScrollAcrossEntitiesQuery } from '@src/graphql/search.generated';
import { Entity, EntityType } from '@src/types.generated';

// Mock the context
vi.mock('@app/entityV2/shared/GlossaryEntityContext', () => ({
    useGlossaryEntityData: vi.fn(),
}));

// Mock the GraphQL query
vi.mock('@src/graphql/search.generated', () => ({
    useScrollAcrossEntitiesQuery: vi.fn(),
    useGetAutoCompleteMultipleResultsQuery: vi.fn(),
}));

vi.mock('@src/graphql/lifecycle.generated', () => ({
    useGetGlossaryNodeCanManageChildrenQuery: vi.fn(),
}));

// Mock the entity registry
vi.mock('@app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({
        getDisplayName: (type: EntityType) => {
            if (type === EntityType.GlossaryTerm) {
                return 'Test Term';
            }
            if (type === EntityType.GlossaryNode) {
                return 'Test Node';
            }
            return '';
        },
    }),
}));

describe('useGlossaryChildren', () => {
    const mockEntity: Entity = {
        urn: 'urn:li:glossaryTerm:test-term',
        type: EntityType.GlossaryTerm,
    };

    const mockNode: Entity = {
        urn: 'urn:li:glossaryNode:test-node',
        type: EntityType.GlossaryNode,
    };

    const mockScrollData = {
        scrollAcrossEntities: {
            searchResults: [{ entity: mockEntity }, { entity: mockNode }],
            nextScrollId: 'next-scroll-id',
        },
    };

    const mockContext = {
        nodeToNewEntity: {},
        setNodeToNewEntity: vi.fn(),
        nodeToDeletedUrn: {},
        setNodeToDeletedUrn: vi.fn(),
        urnsToUpdate: [] as string[],
        setUrnsToUpdate: vi.fn(),
    };

    beforeEach(() => {
        vi.clearAllMocks();
        (useGlossaryEntityData as any).mockReturnValue(mockContext);
        (useScrollAcrossEntitiesQuery as any).mockReturnValue({
            data: undefined,
            loading: false,
        });
        (useGetAutoCompleteMultipleResultsQuery as any).mockReturnValue({
            data: undefined,
            loading: false,
        });
        (useGetGlossaryNodeCanManageChildrenQuery as any).mockReturnValue({
            data: undefined,
            loading: false,
        });
    });

    const mockCanManageChildren = (canManageChildren: boolean) =>
        (useGetGlossaryNodeCanManageChildrenQuery as any).mockReturnValue({
            data: { glossaryNode: { urn: 'test-urn', privileges: { canManageChildren } } },
            loading: false,
        });

    const lastScrollSearchFlags = () => {
        const { calls } = (useScrollAcrossEntitiesQuery as any).mock;
        return calls[calls.length - 1][0].variables.input.searchFlags;
    };

    it('includes draft terms for users who can manage the node', () => {
        mockCanManageChildren(true);
        renderHook(() => useGlossaryChildren({ entityUrn: 'test-urn' }));
        expect(lastScrollSearchFlags().includeHiddenLifecycleStages).toBe(true);
    });

    it('leaves draft terms out for users who cannot manage the node', () => {
        mockCanManageChildren(false);
        renderHook(() => useGlossaryChildren({ entityUrn: 'test-urn' }));
        expect(lastScrollSearchFlags().includeHiddenLifecycleStages).toBeFalsy();
    });

    it('waits for the node privileges before loading children', () => {
        (useGetGlossaryNodeCanManageChildrenQuery as any).mockReturnValue({ data: undefined, loading: true });
        renderHook(() => useGlossaryChildren({ entityUrn: 'test-urn' }));
        expect(useScrollAcrossEntitiesQuery).toHaveBeenLastCalledWith(expect.objectContaining({ skip: true }));
    });

    it('should skip query when entityUrn is not provided', () => {
        const { result } = renderHook(() => useGlossaryChildren({}));

        expect(useScrollAcrossEntitiesQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: true,
            }),
        );
        expect(result.current.data).toEqual([]);
        expect(result.current.loading).toBe(false);
    });

    it('should load initial data', () => {
        (useScrollAcrossEntitiesQuery as any).mockReturnValue({
            data: mockScrollData,
            loading: false,
        });

        const { result } = renderHook(() => useGlossaryChildren({ entityUrn: 'test-urn' }));

        expect(result.current.data).toEqual([mockEntity, mockNode]);
        expect(result.current.loading).toBe(false);
    });

    it('should handle loading state', () => {
        (useScrollAcrossEntitiesQuery as any).mockReturnValue({
            data: undefined,
            loading: true,
        });

        const { result } = renderHook(() => useGlossaryChildren({ entityUrn: 'test-urn' }));

        expect(result.current.loading).toBe(true);
        expect(result.current.data).toEqual([]);
    });

    it('should add new entity when nodeToNewEntity is updated', () => {
        const newEntity: Entity = {
            urn: 'urn:li:glossaryTerm:new-term',
            type: EntityType.GlossaryTerm,
        };

        (useGlossaryEntityData as any).mockReturnValue({
            ...mockContext,
            nodeToNewEntity: { 'test-urn': newEntity },
        });

        const { result } = renderHook(() => useGlossaryChildren({ entityUrn: 'test-urn' }));

        expect(result.current.data).toEqual([newEntity]);
        expect(mockContext.setNodeToNewEntity).toHaveBeenCalledWith(expect.any(Function));
    });
});
