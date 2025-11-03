import { act, renderHook } from '@testing-library/react-hooks';
import { type Mock, vi } from 'vitest';

import usePlatformsModuleUtils from '@app/homeV3/modules/platforms/usePlatformsModuleUtils';
import { PLATFORM_FILTER_NAME } from '@app/search/utils/constants';
import { navigateToSearchUrl } from '@app/searchV2/utils/navigateToSearchUrl';
import { PageRoutes } from '@conf/Global';

// Mocks
const mockPush = vi.fn();
const mockHistory = { push: mockPush };

vi.mock('react-router', () => ({
    useHistory: () => mockHistory,
}));

vi.mock('@app/searchV2/utils/navigateToSearchUrl', () => ({
    navigateToSearchUrl: vi.fn(),
}));

const entityMock = { urn: 'urn:li:entity', type: 'test-type' } as any;

describe('usePlatformsModuleUtils', () => {
    beforeEach(() => {
        mockPush.mockClear();
        (navigateToSearchUrl as unknown as Mock).mockClear();
    });

    it('should provide navigateToDataSources', () => {
        const { result } = renderHook(() => usePlatformsModuleUtils());
        expect(result.current.navigateToDataSources).toBeInstanceOf(Function);
    });

    it('should provide handleEntityClick', () => {
        const { result } = renderHook(() => usePlatformsModuleUtils());
        expect(result.current.handleEntityClick).toBeInstanceOf(Function);
    });

    it('navigateToDataSources should call history.push with INGESTION route', () => {
        const { result } = renderHook(() => usePlatformsModuleUtils());
        act(() => {
            result.current.navigateToDataSources();
        });
        expect(mockPush).toHaveBeenCalledWith({ pathname: PageRoutes.INGESTION });
    });

    it('handleEntityClick should call navigateToSearchUrl with correct params', () => {
        const { result } = renderHook(() => usePlatformsModuleUtils());
        act(() => {
            result.current.handleEntityClick(entityMock);
        });
        expect(navigateToSearchUrl).toHaveBeenCalledWith({
            history: mockHistory,
            filters: [
                {
                    field: PLATFORM_FILTER_NAME,
                    values: [entityMock.urn],
                },
            ],
        });
    });

    it('handleEntityClick should call navigateToSearchUrl with different entity', () => {
        const anotherEntity = { urn: 'urn:li:other-entity', type: 'other-type' } as any;
        const { result } = renderHook(() => usePlatformsModuleUtils());
        act(() => {
            result.current.handleEntityClick(anotherEntity);
        });
        expect(navigateToSearchUrl).toHaveBeenCalledWith({
            history: mockHistory,
            filters: [
                {
                    field: PLATFORM_FILTER_NAME,
                    values: [anotherEntity.urn],
                },
            ],
        });
    });

    it('should not call navigateToSearchUrl if entity is undefined', () => {
        const { result } = renderHook(() => usePlatformsModuleUtils());
        act(() => {
            result.current.handleEntityClick(undefined as any);
        });
        expect(navigateToSearchUrl).not.toHaveBeenCalled();
    });
});
