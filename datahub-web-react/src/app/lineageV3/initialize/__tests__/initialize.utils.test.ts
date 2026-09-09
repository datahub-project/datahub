import { act, renderHook } from '@testing-library/react-hooks';
import { describe, expect, it } from 'vitest';

import {
    BOUNDING_BOX_MEMBER_PAGE_SIZE,
    FetchStatus,
    LINEAGE_FILTER_PAGINATION,
    LineageEntity,
    NodeContext,
} from '@app/lineageV3/common';
import {
    createBoundingBoxMemberNode,
    useBoundingBoxMemberPagination,
} from '@app/lineageV3/initialize/initialize.utils';

import { EntityType, LineageDirection } from '@types';

const ROOT_URN = 'urn:li:semanticModel:(urn:li:dataPlatform:snowflake,model,PROD)';
const OTHER_URN = 'urn:li:semanticModel:(urn:li:dataPlatform:snowflake,other,PROD)';
const MEMBER_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,db.table,PROD)';

function makeNodes(rootUrn: string, boundingBoxLimit?: number): NodeContext['nodes'] {
    const nodes = new Map<string, LineageEntity>();
    nodes.set(rootUrn, {
        id: rootUrn,
        urn: rootUrn,
        type: EntityType.SemanticModel,
        boundingBoxLimit,
    } as LineageEntity);
    return nodes;
}

describe('useBoundingBoxMemberPagination', () => {
    it('starts at zero and is not initialized', () => {
        const { result } = renderHook(() => useBoundingBoxMemberPagination(ROOT_URN, makeNodes(ROOT_URN)));

        expect(result.current.start).toBe(0);
        expect(result.current.initialized).toBe(false);
    });

    it('advances start by page size until target is reached', () => {
        const nodes = makeNodes(ROOT_URN, BOUNDING_BOX_MEMBER_PAGE_SIZE * 3);
        const { result } = renderHook(() => useBoundingBoxMemberPagination(ROOT_URN, nodes));

        act(() => {
            result.current.setTotal(BOUNDING_BOX_MEMBER_PAGE_SIZE * 3);
        });

        expect(result.current.start).toBe(BOUNDING_BOX_MEMBER_PAGE_SIZE * 2);
    });

    it('does not advance start when total fits in one page', () => {
        const { result } = renderHook(() => useBoundingBoxMemberPagination(ROOT_URN, makeNodes(ROOT_URN)));

        act(() => {
            result.current.setTotal(BOUNDING_BOX_MEMBER_PAGE_SIZE - 10);
        });

        expect(result.current.start).toBe(0);
    });

    it('caps paging at boundingBoxLimit when total is larger', () => {
        // A limit part-way into the third page: paging stops there rather than following `total`.
        const limit = BOUNDING_BOX_MEMBER_PAGE_SIZE * 2 + 5;
        const nodes = makeNodes(ROOT_URN, limit);
        const { result } = renderHook(() => useBoundingBoxMemberPagination(ROOT_URN, nodes));

        act(() => {
            result.current.setTotal(BOUNDING_BOX_MEMBER_PAGE_SIZE * 5);
        });

        expect(result.current.start).toBe(BOUNDING_BOX_MEMBER_PAGE_SIZE * 2);
    });

    it('resets start and initialized when rootUrn changes', () => {
        const { result, rerender } = renderHook(
            ({ rootUrn }) => useBoundingBoxMemberPagination(rootUrn, makeNodes(rootUrn)),
            { initialProps: { rootUrn: ROOT_URN } },
        );

        act(() => {
            result.current.setTotal(BOUNDING_BOX_MEMBER_PAGE_SIZE * 3);
            result.current.setInitialized(true);
        });

        rerender({ rootUrn: OTHER_URN });

        expect(result.current.start).toBe(0);
        expect(result.current.initialized).toBe(false);
    });
});

describe('createBoundingBoxMemberNode', () => {
    const entity = { urn: MEMBER_URN, type: EntityType.Dataset };

    it('attaches root bounding-box membership when rootBoundingBoxUrn is provided', () => {
        const node = createBoundingBoxMemberNode(entity, ROOT_URN);

        expect(node.boundingBoxes).toEqual([{ urn: ROOT_URN, isOutputPort: false }]);
    });

    it('leaves boundingBoxes undefined without a root bounding-box urn', () => {
        const node = createBoundingBoxMemberNode(entity);

        expect(node.boundingBoxes).toBeUndefined();
    });

    it('marks member nodes as fully expanded with complete fetch status and paginated filters', () => {
        const node = createBoundingBoxMemberNode(entity, ROOT_URN);

        expect(node.id).toBe(MEMBER_URN);
        expect(node.urn).toBe(MEMBER_URN);
        expect(node.type).toBe(EntityType.Dataset);
        expect(node.isExpanded?.[LineageDirection.Upstream]).toBe(true);
        expect(node.isExpanded?.[LineageDirection.Downstream]).toBe(true);
        expect(node.fetchStatus?.[LineageDirection.Upstream]).toBe(FetchStatus.COMPLETE);
        expect(node.fetchStatus?.[LineageDirection.Downstream]).toBe(FetchStatus.COMPLETE);
        expect(node.filters?.[LineageDirection.Upstream]?.limit).toBe(LINEAGE_FILTER_PAGINATION);
        expect(node.filters?.[LineageDirection.Downstream]?.limit).toBe(LINEAGE_FILTER_PAGINATION);
        expect(node.filters?.[LineageDirection.Upstream]?.facetFilters.size).toBe(0);
        expect(node.filters?.[LineageDirection.Downstream]?.facetFilters.size).toBe(0);
    });
});
