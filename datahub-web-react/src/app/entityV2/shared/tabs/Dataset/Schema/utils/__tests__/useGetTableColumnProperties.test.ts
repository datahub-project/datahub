import { describe, expect, it, vi } from 'vitest';

import { useGetTableColumnProperties } from '@app/entityV2/shared/tabs/Dataset/Schema/utils/useGetTableColumnProperties';
import { renderHook } from '@utils/test-utils/renderHook';

const searchResults = [
    { entity: { urn: 'urn:li:structuredProperty:retention', type: 'STRUCTURED_PROPERTY', definition: {} } },
];
const queryResult = { data: { searchAcrossEntities: { searchResults } } };

vi.mock('@src/graphql/search.generated', () => ({
    useGetSearchResultsForMultipleQuery: () => queryResult,
}));

vi.mock('@src/app/useEntityRegistry', () => ({
    useEntityRegistry: () => ({}),
}));

vi.mock('@src/app/govern/structuredProperties/utils', () => ({
    getEntityTypesPropertyFilter: () => ({}),
    getNotHiddenPropertyFilter: () => ({}),
    getShowInColumnsTablePropertyFilter: () => ({}),
    matchesAllowedPlatforms: () => true,
}));

describe('useGetTableColumnProperties', () => {
    // The schema table builds its columns from this list; a new array on every render would rebuild
    // every column and re-render every visible cell.
    it('returns the same list across renders while the query result is unchanged', () => {
        const { result, rerender } = renderHook(() => useGetTableColumnProperties('urn:li:dataPlatform:hive'));
        const first = result.current;

        rerender();

        expect(first).toEqual(searchResults);
        expect(result.current).toBe(first);
    });
});
