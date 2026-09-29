import { act, renderHook } from '@testing-library/react-hooks';

import useSearchFilterDropdown from '@app/searchV2/filters/useSearchFilterDropdown';
import { STRUCTURED_PROPERTIES_FILTER_NAME } from '@app/searchV2/utils/constants';
import { STRING_TYPE_URN } from '@src/app/shared/constants';

import { FacetMetadata, FilterOperator } from '@types';

// The hook pulls its search context and the aggregation lazy-query from other hooks; stub both so we
// can exercise updateFilters in isolation. updateFilters does not run the aggregation query, so the
// lazy-query mock only needs to satisfy the [trigger, state] tuple shape.
vi.mock('@app/searchV2/useGetSearchQueryInputs', () => ({
    default: () => ({ entityFilters: [], query: '*', orFilters: [], viewUrn: undefined }),
}));

vi.mock('@src/graphql/search.generated', () => ({
    useAggregateAcrossEntitiesLazyQuery: () => [vi.fn(), { data: undefined, loading: false }],
}));

const businessNameField = `${STRUCTURED_PROPERTIES_FILTER_NAME}.datasetBusinessName`;

// A free-form text structured property: STRING valueType with no constrained allowedValues.
function freeFormTextSpFacet(): FacetMetadata {
    return {
        field: businessNameField,
        displayName: 'Dataset Business Name',
        aggregations: [],
        entity: {
            definition: { valueType: { urn: STRING_TYPE_URN } },
        },
    } as unknown as FacetMetadata;
}

describe('useSearchFilterDropdown - updateFilters', () => {
    // Regression guard for the bug where the dropdown showed "contains" but the query sent EQUAL:
    // updateFilters must forward the field's facet to getNewFilters so a free-form text structured
    // property resolves to a CONTAINS condition rather than falling back to the backend default (EQUAL).
    it('applies a CONTAINS condition for free-form text structured properties', () => {
        const onChangeFilters = vi.fn();
        const { result } = renderHook(() =>
            useSearchFilterDropdown({
                filter: freeFormTextSpFacet(),
                activeFilters: [],
                onChangeFilters,
            }),
        );

        act(() => {
            result.current.updateFilters([{ value: 'posted' }]);
        });

        expect(onChangeFilters).toHaveBeenCalledWith([
            expect.objectContaining({
                field: businessNameField,
                values: ['posted'],
                condition: FilterOperator.Contain,
            }),
        ]);
    });
});
