import { describe, expect, it } from 'vitest';

import {
    buildAssertionListFilters,
    convertSortFieldToQueryField,
} from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/AcrylAssertionList';
import { ASSERTION_DEFAULT_FILTERS } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/constant';
import { extractFilterOptionsFromFacets } from '@app/entityV2/shared/tabs/Dataset/Validations/AssertionList/utils';
import {
    ASSERTION_CUSTOM_TYPE_FILTER_NAME,
    ASSERTION_FIELD_PATH_FILTER_NAME,
    ASSERTION_STATUS_FILTER_NAME,
    ASSERTION_TYPE_FILTER_NAME,
    LEGACY_ENTITY_FILTER_NAME,
    OWNERS_FILTER_NAME,
    TAGS_FILTER_NAME,
} from '@app/searchV2/utils/constants';
import {
    AssertionResultType,
    AssertionSourceType,
    AssertionType,
    EntityType,
    FacetMetadata,
    FilterOperator,
} from '@src/types.generated';

describe('convertSortFieldToQueryField', () => {
    it('sorts standard categories by assertion type', () => {
        expect(convertSortFieldToQueryField('type')).toBe(ASSERTION_TYPE_FILTER_NAME);
    });

    it('sorts custom-only categories by their displayed custom type', () => {
        expect(convertSortFieldToQueryField('type', true)).toBe(ASSERTION_CUSTOM_TYPE_FILTER_NAME);
    });
});

describe('buildAssertionListFilters', () => {
    it('builds entity and selected facet filters', () => {
        const filters = buildAssertionListFilters(
            {
                ...ASSERTION_DEFAULT_FILTERS,
                filterCriteria: {
                    searchText: '',
                    status: [AssertionResultType.Failure],
                    type: [AssertionType.Field],
                    source: [AssertionSourceType.Native],
                    tags: ['urn:li:tag:Important'],
                    column: ['event_id'],
                    owners: ['urn:li:corpuser:datahub'],
                },
            },
            ['urn:li:dataset:test', 'urn:li:dataset:sibling'],
        );

        expect(filters).toHaveLength(1);
        expect(filters[0].and).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    field: LEGACY_ENTITY_FILTER_NAME,
                    values: ['urn:li:dataset:test', 'urn:li:dataset:sibling'],
                }),
                expect.objectContaining({
                    field: ASSERTION_STATUS_FILTER_NAME,
                    values: ['FAILING'],
                    condition: FilterOperator.Equal,
                }),
                expect.objectContaining({
                    field: TAGS_FILTER_NAME,
                    values: ['urn:li:tag:Important'],
                }),
                expect.objectContaining({
                    field: ASSERTION_FIELD_PATH_FILTER_NAME,
                    values: ['event_id'],
                }),
                expect.objectContaining({
                    field: OWNERS_FILTER_NAME,
                    values: ['urn:li:corpuser:datahub'],
                }),
            ]),
        );
    });

    it('represents external assertions as non-native and non-inferred', () => {
        const filters = buildAssertionListFilters(
            {
                ...ASSERTION_DEFAULT_FILTERS,
                filterCriteria: {
                    ...ASSERTION_DEFAULT_FILTERS.filterCriteria,
                    source: [AssertionSourceType.External],
                },
            },
            ['urn:li:dataset:test'],
        );

        expect(filters[0].and).toEqual(
            expect.arrayContaining([
                expect.objectContaining({
                    field: 'sourceType',
                    values: [AssertionSourceType.Native, AssertionSourceType.Inferred],
                    condition: FilterOperator.Equal,
                    negated: true,
                }),
            ]),
        );
    });

    it('uses tag urns as values and tag names as labels', () => {
        const options = extractFilterOptionsFromFacets([], [
            {
                field: TAGS_FILTER_NAME,
                aggregations: [
                    {
                        value: 'urn:li:tag:Important',
                        count: 3,
                        entity: {
                            type: EntityType.Tag,
                            name: 'Important',
                            properties: { name: 'Important' },
                        },
                    },
                ],
            },
        ] as unknown as FacetMetadata[]);

        expect(options.filterGroupOptions.tags).toEqual([
            expect.objectContaining({
                name: 'urn:li:tag:Important',
                displayName: 'Important',
                count: 3,
            }),
        ]);
    });
});
