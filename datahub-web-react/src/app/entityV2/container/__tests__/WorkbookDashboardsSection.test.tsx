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

describe('WorkbookDashboardsSection', () => {
    it("lists the workbook's sheets without the dashboard that stands for the workbook", () => {
        vi.mocked(useEntityData).mockReturnValue({ urn: 'urn:li:container:app' } as any);
        vi.mocked(useGetSearchResultsForMultipleQuery).mockReturnValue({ data: undefined } as any);

        render(<WorkbookDashboardsSection />);

        const { variables } = vi.mocked(useGetSearchResultsForMultipleQuery).mock.calls[0][0] as any;
        expect(variables.input.orFilters).toEqual([
            {
                and: [
                    { field: CONTAINER_FILTER_NAME, values: ['urn:li:container:app'] },
                    { field: TYPE_NAMES_FILTER_NAME, values: ['App'], negated: true },
                ],
            },
        ]);
    });
});
