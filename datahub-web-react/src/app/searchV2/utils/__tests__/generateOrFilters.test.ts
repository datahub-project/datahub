import { FrontendFilterOperator } from '@app/searchV2/filters/types';
import {
    DOMAINS_FILTER_NAME,
    ENTITY_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    TAGS_FILTER_NAME,
    UnionType,
} from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';
import { FilterOperator } from '@src/types.generated';

describe('generateOrFilters', () => {
    it('should generate orFilters with UnionType.AND', () => {
        const filters = [
            { field: ENTITY_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
            { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
        ];
        const orFilters = generateOrFilters(UnionType.AND, filters);

        expect(orFilters).toMatchObject([
            {
                and: filters,
            },
        ]);
    });

    it('should generate orFilters with UnionType.OR', () => {
        const filters = [
            { field: ENTITY_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
            { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
        ];
        const orFilters = generateOrFilters(UnionType.OR, filters);

        expect(orFilters).toMatchObject([
            {
                and: [filters[0]],
            },
            {
                and: [filters[1]],
            },
        ]);
    });

    it('should generate orFilters with UnionType.AND and nestedFilters', () => {
        const filters = [
            { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
            { field: DOMAINS_FILTER_NAME, values: ['urn:li:domains:domain1'] },
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['CONTAINER', 'DATASET␞table'] },
        ];
        // const nestedFilters = [{ field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['CONTAINER', 'DATASET␞table'] }];
        const orFilters = generateOrFilters(UnionType.AND, filters);

        expect(orFilters).toMatchObject([
            {
                and: [
                    { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
                    { field: DOMAINS_FILTER_NAME, values: ['urn:li:domains:domain1'] },
                    { field: '_entityType', values: ['CONTAINER'] },
                ],
            },
            {
                and: [
                    { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
                    { field: DOMAINS_FILTER_NAME, values: ['urn:li:domains:domain1'] },
                    { field: '_entityType', values: ['DATASET'] },
                    { field: 'typeNames', values: ['table'] },
                ],
            },
        ]);
    });

    it('should generate orFilters with UnionType.OR and nestedFilters', () => {
        const filters = [
            { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
            { field: DOMAINS_FILTER_NAME, values: ['urn:li:domains:domain1'] },
            { field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['CONTAINER', 'DATASET␞table'] },
        ];
        const orFilters = generateOrFilters(UnionType.OR, filters);

        expect(orFilters).toMatchObject([
            {
                and: [{ field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] }],
            },
            {
                and: [{ field: DOMAINS_FILTER_NAME, values: ['urn:li:domains:domain1'] }],
            },
            {
                and: [{ field: '_entityType', values: ['CONTAINER'] }],
            },
            {
                and: [
                    { field: '_entityType', values: ['DATASET'] },
                    { field: 'typeNames', values: ['table'] },
                ],
            },
        ]);
    });

    it('should generate orFilters and exclude filters with a provided exclude field', () => {
        const filters = [
            { field: ENTITY_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
            { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] },
        ];
        const orFilters = generateOrFilters(UnionType.AND, filters, [ENTITY_FILTER_NAME]);

        expect(orFilters).toMatchObject([
            {
                and: [{ field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'] }],
            },
        ]);
    });

    it('should split a criterion if the operator is ALL_EQUALS', () => {
        const filters = [
            {
                field: TAGS_FILTER_NAME,
                values: ['urn:li:tag:tag1', 'urn:li:tag:tag2', 'urn:li:tag:tag3'],
                condition: FrontendFilterOperator.AllEqual,
            },
        ];
        const orFilters = generateOrFilters(UnionType.AND, filters);

        expect(orFilters).toMatchObject([
            {
                and: [
                    { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag1'], condition: FilterOperator.Equal },
                    { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag2'], condition: FilterOperator.Equal },
                    { field: TAGS_FILTER_NAME, values: ['urn:li:tag:tag3'], condition: FilterOperator.Equal },
                ],
            },
        ]);
    });

    it('should split a criterion if the operator is ALL_EQUALS along with other filters', () => {
        const filters = [
            { field: ENTITY_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
            {
                field: TAGS_FILTER_NAME,
                values: ['urn:li:tag:tag1', 'urn:li:tag:tag2', 'urn:li:tag:tag3'],
                condition: FrontendFilterOperator.AllEqual,
            },
        ];
        const orFilters = generateOrFilters(UnionType.AND, filters);

        expect(orFilters).toMatchObject([
            {
                and: [
                    { field: ENTITY_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
                    {
                        field: TAGS_FILTER_NAME,
                        values: ['urn:li:tag:tag1'],
                        condition: FilterOperator.Equal,
                    },
                    {
                        field: TAGS_FILTER_NAME,
                        values: ['urn:li:tag:tag2'],
                        condition: FilterOperator.Equal,
                    },
                    {
                        field: TAGS_FILTER_NAME,
                        values: ['urn:li:tag:tag3'],
                        condition: FilterOperator.Equal,
                    },
                ],
            },
        ]);
    });
});
