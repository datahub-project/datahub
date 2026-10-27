import {
    DOCUMENT_CREATOR_FILTER_NAME,
    DOCUMENT_STATE_FILTER_NAME,
    buildDocumentSidebarFilters,
} from '@app/document/utils/documentSidebarFilters';
import {
    DOMAINS_FILTER_NAME,
    GLOSSARY_TERMS_FILTER_NAME,
    PLATFORM_FILTER_NAME,
    TAGS_FILTER_NAME,
    TYPE_NAMES_FILTER_NAME,
    UnionType,
} from '@app/searchV2/utils/constants';
import { generateOrFilters } from '@app/searchV2/utils/generateOrFilters';

import { FilterOperator } from '@types';

describe('buildDocumentSidebarFilters', () => {
    it('returns empty when nothing is selected', () => {
        expect(buildDocumentSidebarFilters({})).toEqual([]);
        expect(buildDocumentSidebarFilters({ status: 'all' })).toEqual([]);
    });

    it('emits a filter for each non-empty selection', () => {
        expect(
            buildDocumentSidebarFilters({
                typeNames: ['runbook'],
                domainUrns: ['urn:li:domain:eng'],
                tagUrns: ['urn:li:tag:pii'],
                termUrns: ['urn:li:glossaryTerm:revenue'],
                authorUrns: ['urn:li:corpuser:jane'],
                platformUrns: ['urn:li:dataPlatform:notion'],
                status: 'published',
            }),
        ).toEqual([
            { field: TYPE_NAMES_FILTER_NAME, condition: FilterOperator.Equal, values: ['runbook'] },
            { field: DOMAINS_FILTER_NAME, condition: FilterOperator.Equal, values: ['urn:li:domain:eng'] },
            { field: TAGS_FILTER_NAME, condition: FilterOperator.Equal, values: ['urn:li:tag:pii'] },
            {
                field: GLOSSARY_TERMS_FILTER_NAME,
                condition: FilterOperator.Equal,
                values: ['urn:li:glossaryTerm:revenue'],
            },
            {
                field: DOCUMENT_CREATOR_FILTER_NAME,
                condition: FilterOperator.Equal,
                values: ['urn:li:corpuser:jane'],
            },
            {
                field: PLATFORM_FILTER_NAME,
                condition: FilterOperator.Equal,
                values: ['urn:li:dataPlatform:notion'],
            },
            { field: DOCUMENT_STATE_FILTER_NAME, condition: FilterOperator.Equal, values: ['PUBLISHED'] },
        ]);
    });

    it('maps unpublished status', () => {
        expect(buildDocumentSidebarFilters({ status: 'unpublished' })).toEqual([
            { field: DOCUMENT_STATE_FILTER_NAME, condition: FilterOperator.Equal, values: ['UNPUBLISHED'] },
        ]);
    });

    it('ANDs with generateOrFilters like global search', () => {
        const filters = buildDocumentSidebarFilters({
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
