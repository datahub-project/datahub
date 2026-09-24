import { renderHook } from '@testing-library/react-hooks';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { useGetOutputPorts } from '@app/entityV2/summary/modules/outputPorts/useGetOutputPorts';
import { useModuleContext } from '@app/homeV3/module/context/ModuleContext';
import { OUTPUT_PORTS_FIELD } from '@app/search/utils/constants';

import { useListDataProductAssetsQuery } from '@graphql/search.generated';
import { EntityType } from '@types';

vi.mock('@app/entity/shared/EntityContext', () => ({
    useEntityData: vi.fn(),
}));
vi.mock('@app/homeV3/module/context/ModuleContext', () => ({
    useModuleContext: vi.fn(),
}));
vi.mock('@graphql/search.generated', () => ({
    useListDataProductAssetsQuery: vi.fn(),
}));

describe('useGetOutputPorts', () => {
    const urn = 'urn:li:dataProduct:1';
    const refetch = vi.fn();

    beforeEach(() => {
        (useEntityData as unknown as any).mockReturnValue({ urn, entityType: EntityType.DataProduct });
        (useModuleContext as unknown as any).mockReturnValue({ isReloading: false, onReloadingFinished: vi.fn() });
        // Re-establish after afterEach's resetAllMocks so refetch resolves in every test.
        refetch.mockResolvedValue({
            data: {
                listDataProductAssets: {
                    searchResults: [{ entity: { urn: 'urn:li:dataset:port3', type: 'DATASET' } }],
                },
            },
        });
        (useListDataProductAssetsQuery as unknown as any).mockReturnValue({
            loading: false,
            data: {
                listDataProductAssets: {
                    searchResults: [
                        { entity: { urn: 'urn:li:dataset:port1', type: 'DATASET' } },
                        { entity: { urn: 'urn:li:dataset:port2', type: 'DATASET' } },
                    ],
                    total: 2,
                },
            },
            refetch,
        });
    });

    afterEach(() => {
        vi.resetAllMocks();
    });

    it('queries output ports only (isOutputPort=true), scoped to the data product', () => {
        renderHook(() => useGetOutputPorts());

        const args = (useListDataProductAssetsQuery as unknown as any).mock.calls[0][0];
        expect(args.variables.urn).toBe(urn);
        expect(args.variables.input.filters).toEqual([{ field: OUTPUT_PORTS_FIELD, value: 'true' }]);
    });

    it('returns total and the first page of entities without refetching', async () => {
        const { result } = renderHook(() => useGetOutputPorts());

        expect(result.current.loading).toBe(false);
        expect(result.current.total).toBe(2);

        const entities = await result.current.fetchOutputPorts(0, 10);
        expect(entities.map((e) => e.urn)).toEqual(['urn:li:dataset:port1', 'urn:li:dataset:port2']);
        expect(refetch).not.toHaveBeenCalled();
    });

    it('refetches for subsequent pages', async () => {
        const { result } = renderHook(() => useGetOutputPorts());

        const entities = await result.current.fetchOutputPorts(10, 10);
        expect(refetch).toHaveBeenCalled();
        expect(entities.map((e) => e.urn)).toEqual(['urn:li:dataset:port3']);
    });

    it('is loading while the query is in flight', () => {
        (useListDataProductAssetsQuery as unknown as any).mockReturnValue({
            loading: true,
            data: undefined,
            refetch,
        });

        const { result } = renderHook(() => useGetOutputPorts());
        expect(result.current.loading).toBe(true);
        expect(result.current.total).toBe(0);
    });
});
