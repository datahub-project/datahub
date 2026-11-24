import { render } from '@testing-library/react';
import React from 'react';
import { vi } from 'vitest';

import WorkbookDashboardsSection from '@app/entityV2/container/workbook/WorkbookDashboardsSection';
import { useEntityData } from '@app/entityV2/shared/EntityContext';
import { CONTAINER_FILTER_NAME, TYPE_NAMES_FILTER_NAME } from '@app/searchV2/utils/constants';

import { useGetSearchResultsForMultipleQuery } from '@graphql/search.generated';

vi.mock('@app/entityV2/shared/EntityContext', () => ({ useEntityData: vi.fn() }));
vi.mock('@app/useEntityRegistry', () => ({ useEntityRegistry: () => ({ getCollectionName: () => 'Dashboards' }) }));
vi.mock('@graphql/search.generated', async (importOriginal) => ({
    ...(await importOriginal<typeof import('@graphql/search.generated')>()),
    useGetSearchResultsForMultipleQuery: vi.fn(),
}));

function searchFilters(platform: string) {
    vi.mocked(useEntityData).mockReturnValue({
        urn: 'urn:li:container:app',
        entityData: { platform: { name: platform } },
    } as any);
    vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({ data: undefined } as any);
    render(<WorkbookDashboardsSection />);
    const { variables } = vi.mocked(useGetSearchResultsForMultipleQuery).mock.calls.at(-1)?.[0] as any;
    return variables.input.orFilters;
}

describe('WorkbookDashboardsSection', () => {
    it("lists a Qlik app's sheets without the app's own dashboard", () => {
        expect(searchFilters('qlik-sense')).toEqual([
            {
                and: [
                    { field: CONTAINER_FILTER_NAME, values: ['urn:li:container:app'] },
                    { field: TYPE_NAMES_FILTER_NAME, values: ['App'], negated: true },
                ],
            },
        ]);
    });

    it("lists every dashboard of another platform's workbook", () => {
        expect(searchFilters('sigma')).toEqual([
            { and: [{ field: CONTAINER_FILTER_NAME, values: ['urn:li:container:app'] }] },
        ]);
    });
});
