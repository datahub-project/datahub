import {
    DOMAINS_FILTER_NAME,
    ENTITY_SUB_TYPE_FILTER_NAME,
    ENTITY_TYPE_FILTER_NAME,
    TAGS_FILTER_NAME,
    UnionType,
} from '../constants';
import { generateOrFilters } from '../generateOrFilters';

describe('generateOrFilters', () => {
    it('should generate orFilters with UnionType.AND', () => {
        const filters = [
            { field: ENTITY_TYPE_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
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
            { field: ENTITY_TYPE_FILTER_NAME, values: ['DATASET', 'CONTAINER'] },
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
        ];
        const nestedFilters = [{ field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['CONTAINER', 'DATASET␞table'] }];
        const orFilters = generateOrFilters(UnionType.AND, filters, nestedFilters);

        expect(orFilters).toMatchObject([
            {
                and: [...filters, { field: '_entityType', values: ['CONTAINER'] }],
            },
            {
                and: [
                    ...filters,
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
        ];
        const nestedFilters = [{ field: ENTITY_SUB_TYPE_FILTER_NAME, values: ['CONTAINER', 'DATASET␞table'] }];
        const orFilters = generateOrFilters(UnionType.OR, filters, nestedFilters);

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
});
