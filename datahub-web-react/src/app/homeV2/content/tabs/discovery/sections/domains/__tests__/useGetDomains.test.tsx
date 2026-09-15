import { renderHook } from '@testing-library/react-hooks';
import { Mock, beforeEach, describe, expect, it, vi } from 'vitest';

import { useGetDomains } from '@app/homeV2/content/tabs/discovery/sections/domains/useGetDomains';
import { useHomeRecommendations } from '@app/homeV2/useHomeRecommendations';

import { useListRecommendationsQuery } from '@graphql/recommendations.generated';
import { EntityType } from '@types';

vi.mock('@app/homeV2/useHomeRecommendations', () => ({
    useHomeRecommendations: vi.fn(),
}));

vi.mock('@graphql/recommendations.generated', () => ({
    useListRecommendationsQuery: vi.fn(),
}));

describe('useGetDomains', () => {
    const homeRecsMock = useHomeRecommendations as unknown as Mock;
    const queryMock = useListRecommendationsQuery as unknown as Mock;

    beforeEach(() => {
        vi.clearAllMocks();
        homeRecsMock.mockReturnValue({
            modules: undefined,
            loading: false,
            refetch: vi.fn(),
        });
    });

    it('does not fire its own listRecommendations query', () => {
        renderHook(() => useGetDomains());
        expect(queryMock).not.toHaveBeenCalled();
        expect(homeRecsMock).toHaveBeenCalled();
    });

    it('maps the Domains recommendation module', () => {
        const domain = { urn: 'urn:li:domain:1', type: EntityType.Domain };
        homeRecsMock.mockReturnValue({
            modules: [
                {
                    moduleId: 'Platforms',
                    content: [{ entity: { urn: 'urn:li:dataPlatform:snowflake', type: EntityType.DataPlatform } }],
                },
                {
                    moduleId: 'Domains',
                    content: [{ entity: domain, params: { contentParams: { count: 4 } } }],
                },
            ],
            loading: false,
            refetch: vi.fn(),
        });

        const { result } = renderHook(() => useGetDomains());

        expect(result.current.domains).toEqual([{ entity: domain, assetCount: 4 }]);
        expect(result.current.loading).toBe(false);
    });
});
