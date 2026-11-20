import { act } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import useDataProductChildren from '@app/entityV2/shared/EntityDropdown/useDataProductChildren';
import { GenericEntityProperties } from '@app/entityV2/shared/types';
import { renderHook } from '@utils/test-utils/renderHook';

import { EntityType } from '@types';

const fetchChildrenMock = vi.fn();
let lazyQueryData: unknown;
let lazyQueryError: Error | undefined;
let lazyQueryVariables: { urn: string } | undefined;
vi.mock('@graphql/dataProduct.generated', () => ({
    useGetDataProductChildrenCountLazyQuery: () => [
        fetchChildrenMock,
        { data: lazyQueryData, error: lazyQueryError, variables: lazyQueryVariables },
    ],
}));

const URN = 'urn:li:dataProduct:parent';
const CARD_DATA: GenericEntityProperties = { name: 'Finance' };

describe('useDataProductChildren', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        lazyQueryData = undefined;
        lazyQueryError = undefined;
        lazyQueryVariables = undefined;
    });

    it('fetches the child count when a data product card menu opens', () => {
        const { result } = renderHook(() => useDataProductChildren(URN, EntityType.DataProduct, CARD_DATA));

        act(() => result.current.onMenuOpenChange(true));

        expect(fetchChildrenMock).toHaveBeenCalledWith({ variables: { urn: URN } });
    });

    it('adds the fetched count to the entity data', () => {
        lazyQueryData = { dataProduct: { urn: URN, childDataProducts: { total: 2 } } };

        const { result } = renderHook(() => useDataProductChildren(URN, EntityType.DataProduct, CARD_DATA));

        expect(result.current.entityData?.children?.total).toBe(2);
        expect(result.current.entityData?.name).toBe('Finance');
        expect(result.current.childCountPending).toBe(false);
    });

    it('reports the count as pending until it arrives, and not after a failed lookup', () => {
        const pending = renderHook(() => useDataProductChildren(URN, EntityType.DataProduct, CARD_DATA));
        expect(pending.result.current.childCountPending).toBe(true);

        lazyQueryError = new Error('lookup failed');
        lazyQueryVariables = { urn: URN };
        const failed = renderHook(() => useDataProductChildren(URN, EntityType.DataProduct, CARD_DATA));
        expect(failed.result.current.childCountPending).toBe(false);
    });

    it('keeps the count pending when the failed lookup was for another data product', () => {
        lazyQueryError = new Error('lookup failed');
        lazyQueryVariables = { urn: 'urn:li:dataProduct:other' };

        const { result } = renderHook(() => useDataProductChildren(URN, EntityType.DataProduct, CARD_DATA));

        expect(result.current.childCountPending).toBe(true);
    });

    it('ignores a count fetched for another data product', () => {
        lazyQueryData = { dataProduct: { urn: 'urn:li:dataProduct:other', childDataProducts: { total: 3 } } };

        const { result } = renderHook(() => useDataProductChildren(URN, EntityType.DataProduct, CARD_DATA));

        expect(result.current.entityData?.children).toBeUndefined();
        expect(result.current.childCountPending).toBe(true);
    });

    it('does not fetch when the count is already known or the entity is not a data product', () => {
        const profileData = { ...CARD_DATA, children: { relationships: [], total: 1 } };
        const profile = renderHook(() => useDataProductChildren(URN, EntityType.DataProduct, profileData));
        const domain = renderHook(() => useDataProductChildren('urn:li:domain:d', EntityType.Domain, CARD_DATA));

        act(() => {
            profile.result.current.onMenuOpenChange(true);
            domain.result.current.onMenuOpenChange(true);
        });

        expect(fetchChildrenMock).not.toHaveBeenCalled();
        expect(profile.result.current.entityData).toBe(profileData);
        expect(profile.result.current.childCountPending).toBe(false);
        expect(domain.result.current.childCountPending).toBe(false);
    });
});
