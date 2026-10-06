import { buildMetricsSidebarFilters } from '@app/metrics/utils/metricsSidebarFilters';
import {
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    OWNERS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    UnionType,
} from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';

import { FilterOperator } from '@types';

describe('buildMetricsSidebarFilters', () => {
    it('returns empty when nothing is selected', () => {
        expect(buildMetricsSidebarFilters({})).toEqual([]);
    });

    it('emits a filter for each non-empty selection', () => {
        expect(
            buildMetricsSidebarFilters({
                platformUrns: ['urn:li:dataPlatform:dbt'],
                domainUrns: ['urn:li:domain:eng'],
                tagUrns: ['urn:li:tag:pii'],
                termUrns: ['urn:li:glossaryTerm:revenue'],
                ownerUrns: ['urn:li:corpuser:jane'],
            }),
        ).toEqual([
            {
                field: PLATFORM_FILTER_NAME,
                condition: FilterOperator.Equal,
                values: ['urn:li:dataPlatform:dbt'],
            },
            { field: DOMAINS_FILTER_NAME, condition: FilterOperator.Equal, values: ['urn:li:domain:eng'] },
            { field: TAGS_FILTER_NAME, condition: FilterOperator.Equal, values: ['urn:li:tag:pii'] },
            {
                field: GLOSSARY_TERMS_FILTER_NAME,
                condition: FilterOperator.Equal,
                values: ['urn:li:glossaryTerm:revenue'],
            },
            {
                field: OWNERS_FILTER_NAME,
                condition: FilterOperator.Equal,
                values: ['urn:li:corpuser:jane'],
            },
        ]);
    });

    it('ANDs with generateOrFilters like global search', () => {
        const filters = buildMetricsSidebarFilters({
            domainUrns: ['urn:li:domain:engineering'],
            tagUrns: ['urn:li:tag:pii'],
        });
        const orFilters = generateOrFilters(UnionType.AND, filters);

        expect(orFilters).toHaveLength(1);
        expect(orFilters[0].and).toEqual(
            expect.arrayContaining([
                expect.objectContaining({ field: DOMAINS_FILTER_NAME, values: ['urn:li:domain:engineering'] }),
                expect.objectContaining({ field: TAGS_FILTER_NAME, values: ['urn:li:tag:pii'] }),
            ]),
        );
    });
});
