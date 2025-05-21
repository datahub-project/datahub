import { convertFacetsToFieldToFacetStateMap } from '@app/searchV2/filtersV2/utils';
import { FacetMetadata } from '@src/types.generated';

describe('convertFacetsToFieldToFacetStateMap', () => {
    const mockFacets: FacetMetadata[] = [
        {
            __typename: 'FacetMetadata',
            field: 'field1',
            aggregations: [
                { count: 5, value: 'value1' },
                { count: 10, value: 'value2' },
            ],
        },
        {
            __typename: 'FacetMetadata',
            field: 'field2',
            aggregations: [{ count: 3, value: 'value3' }],
        },
        {
            __typename: 'FacetMetadata',
            field: 'field3',
            aggregations: [],
        },
    ];

    it('should return undefined when facets is undefined', () => {
        const result = convertFacetsToFieldToFacetStateMap(undefined);
        expect(result).toBeUndefined();
    });

    it('should return an empty map when facets is an empty array', () => {
        const result = convertFacetsToFieldToFacetStateMap([]);
        expect(result).toEqual(new Map());
    });

    it('should correctly map facets to a FieldToFacetStateMap', () => {
        const result = convertFacetsToFieldToFacetStateMap(mockFacets);

        // Expected output
        const expectedMap = new Map<string, { facet: FacetMetadata; loading: boolean }>([
            [
                'field1',
                {
                    facet: {
                        __typename: 'FacetMetadata',
                        field: 'field1',
                        aggregations: [
                            { count: 5, value: 'value1' },
                            { count: 10, value: 'value2' },
                        ],
                    },
                    loading: false,
                },
            ],
            [
                'field2',
                {
                    facet: {
                        __typename: 'FacetMetadata',
                        field: 'field2',
                        aggregations: [{ count: 3, value: 'value3' }],
                    },
                    loading: false,
                },
            ],
            [
                'field3',
                {
                    facet: {
                        __typename: 'FacetMetadata',
                        field: 'field3',
                        aggregations: [],
                    },
                    loading: false,
                },
            ],
        ]);

        expect(result).toEqual(expectedMap);
    });

    it('should handle facets with unique fields correctly', () => {
        const uniqueFacets: FacetMetadata[] = [
            {
                __typename: 'FacetMetadata',
                field: 'uniqueField1',
                aggregations: [{ count: 7, value: 'uniqueValue1' }],
            },
            {
                __typename: 'FacetMetadata',
                field: 'uniqueField2',
                aggregations: [{ count: 2, value: 'uniqueValue2' }],
            },
        ];
        const result = convertFacetsToFieldToFacetStateMap(uniqueFacets);

        const expectedMap = new Map<string, { facet: FacetMetadata; loading: boolean }>([
            [
                'uniqueField1',
                {
                    facet: {
                        __typename: 'FacetMetadata',
                        field: 'uniqueField1',
                        aggregations: [{ count: 7, value: 'uniqueValue1' }],
                    },
                    loading: false,
                },
            ],
            [
                'uniqueField2',
                {
                    facet: {
                        __typename: 'FacetMetadata',
                        field: 'uniqueField2',
                        aggregations: [{ count: 2, value: 'uniqueValue2' }],
                    },
                    loading: false,
                },
            ],
        ]);

        expect(result).toEqual(expectedMap);
    });
});
