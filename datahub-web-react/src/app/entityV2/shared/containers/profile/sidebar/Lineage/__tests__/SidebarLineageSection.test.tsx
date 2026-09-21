import { ApolloLink, InMemoryCache, Observable } from '@apollo/client';
import { MockedProvider } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';

import { EntityContext } from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import SidebarLineageSection from '@app/entityV2/shared/containers/profile/sidebar/Lineage/SidebarLineageSection';
import { TabContextType } from '@app/entityV2/shared/types';
import EntitySidebarContext, { SearchResultLineageCounts } from '@app/sharedV2/EntitySidebarContext';
import possibleTypesResult from '@src/possibleTypes.generated';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { GetSearchAcrossLineageCountsDocument } from '@graphql/lineage.generated';
import { EntityType } from '@types';

const URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)';

const entityData: GenericEntityProperties = {
    urn: URN,
    type: EntityType.Dataset,
};

function textContent(expected: string) {
    return (_: string, element: Element | null) => element?.textContent === expected;
}

function typeFacets(entityType: string, count: number) {
    return [
        {
            __typename: 'FacetMetadata',
            field: '_entityType',
            displayName: 'Type',
            entity: null,
            aggregations: [
                {
                    __typename: 'AggregationMetadata',
                    value: entityType,
                    count,
                    entity: null,
                },
            ],
        },
    ];
}

function renderSection({
    contextType,
    operations,
    searchResultLineage,
    writeTypeBreakdown,
    writeEmptyTypeBreakdown,
}: {
    contextType: TabContextType;
    operations: string[];
    searchResultLineage?: SearchResultLineageCounts | null;
    writeTypeBreakdown?: boolean;
    writeEmptyTypeBreakdown?: boolean;
}) {
    const cache = new InMemoryCache({
        possibleTypes: possibleTypesResult.possibleTypes,
    });
    if (writeTypeBreakdown || writeEmptyTypeBreakdown) {
        cache.writeQuery({
            query: GetSearchAcrossLineageCountsDocument,
            variables: { urn: URN, startTimeMillis: null },
            data: writeEmptyTypeBreakdown
                ? {
                      upstreams: {
                          __typename: 'SearchAcrossLineageResults',
                          start: 0,
                          count: 0,
                          total: 0,
                          facets: [],
                      },
                      downstreams: {
                          __typename: 'SearchAcrossLineageResults',
                          start: 0,
                          count: 0,
                          total: 0,
                          facets: [],
                      },
                  }
                : {
                      upstreams: {
                          __typename: 'SearchAcrossLineageResults',
                          start: 0,
                          count: 2,
                          total: 2,
                          facets: typeFacets('DATASET', 2),
                      },
                      downstreams: {
                          __typename: 'SearchAcrossLineageResults',
                          start: 0,
                          count: 1,
                          total: 1,
                          facets: typeFacets('CHART', 1),
                      },
                  },
        });
    }
    const link = new ApolloLink(
        (operation) =>
            new Observable((observer) => {
                operations.push(operation.operationName);
                if (operation.operationName === 'getLineageCounts') {
                    observer.next({
                        data: {
                            entity: {
                                __typename: 'Dataset',
                                urn: URN,
                                type: EntityType.Dataset,
                                upstream: { __typename: 'EntityLineageResult', filtered: 1, total: 3 },
                                downstream: { __typename: 'EntityLineageResult', filtered: 0, total: 4 },
                            },
                        },
                    });
                } else if (operation.operationName === 'getSearchAcrossLineageCounts') {
                    observer.next({
                        data: {
                            upstreams: {
                                __typename: 'SearchAcrossLineageResults',
                                start: 0,
                                count: 0,
                                total: 0,
                                facets: [],
                            },
                            downstreams: {
                                __typename: 'SearchAcrossLineageResults',
                                start: 0,
                                count: 0,
                                total: 0,
                                facets: [],
                            },
                        },
                    });
                } else {
                    observer.next({ data: {} });
                }
                observer.complete();
            }),
    );

    return render(
        <MockedProvider cache={cache} link={link}>
            <TestPageContainer>
                <EntitySidebarContext.Provider
                    value={{
                        width: 400,
                        isClosed: false,
                        setSidebarClosed: vi.fn(),
                        searchResultLineage,
                    }}
                >
                    <EntityContext.Provider
                        value={{
                            urn: URN,
                            entityType: EntityType.Dataset,
                            entityData,
                            loading: false,
                            baseEntity: null,
                            updateEntity: vi.fn(),
                            routeToTab: vi.fn(),
                            refetch: vi.fn(),
                            lineage: undefined,
                            dataNotCombinedWithSiblings: null,
                        }}
                    >
                        <SidebarLineageSection contexType={contextType} />
                    </EntityContext.Provider>
                </EntitySidebarContext.Provider>
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('SidebarLineageSection', () => {
    it('falls back to generic asset counts without a type breakdown cache', async () => {
        const operations: string[] = [];
        renderSection({ contextType: TabContextType.SEARCH_SIDEBAR, operations });

        expect(await screen.findByText('UPSTREAM')).toBeInTheDocument();
        expect(screen.getByText('DOWNSTREAM')).toBeInTheDocument();
        expect(await screen.findByText(textContent('Depends on 2 assets'))).toBeInTheDocument();
        expect(screen.getByText(textContent('Used by 4 assets'))).toBeInTheDocument();
        expect(operations.filter((operation) => operation.toLowerCase().includes('lineage'))).toEqual([
            'getLineageCounts',
        ]);
    });

    it('reuses search-result lineage counts without a network request', async () => {
        const operations: string[] = [];
        renderSection({
            contextType: TabContextType.SEARCH_SIDEBAR,
            operations,
            searchResultLineage: {
                urn: URN,
                upstream: { filtered: 0, total: 1 },
                downstream: { filtered: 0, total: 3 },
            },
        });

        expect(await screen.findByText(textContent('Depends on 1 asset'))).toBeInTheDocument();
        expect(screen.getByText(textContent('Used by 3 assets'))).toBeInTheDocument();
        expect(operations.filter((operation) => operation.toLowerCase().includes('lineage'))).toEqual([]);
    });

    it('uses a cached type breakdown when the detailed query is already in Apollo', async () => {
        const operations: string[] = [];
        renderSection({
            contextType: TabContextType.SEARCH_SIDEBAR,
            operations,
            writeTypeBreakdown: true,
        });

        expect(await screen.findByText(textContent('Depends on 2 datasets'))).toBeInTheDocument();
        expect(screen.getByText(textContent('Used by 1 chart'))).toBeInTheDocument();
        expect(operations.filter((operation) => operation.toLowerCase().includes('lineage'))).toEqual([]);
    });

    it('ignores empty cached type breakdowns and still loads counts', async () => {
        const operations: string[] = [];
        renderSection({
            contextType: TabContextType.SEARCH_SIDEBAR,
            operations,
            writeEmptyTypeBreakdown: true,
        });

        expect(await screen.findByText(textContent('Depends on 2 assets'))).toBeInTheDocument();
        expect(screen.getByText(textContent('Used by 4 assets'))).toBeInTheDocument();
        expect(operations.filter((operation) => operation.toLowerCase().includes('lineage'))).toEqual([
            'getLineageCounts',
        ]);
    });

    it('ignores search-result lineage that belongs to a different urn', async () => {
        const operations: string[] = [];
        renderSection({
            contextType: TabContextType.SEARCH_SIDEBAR,
            operations,
            searchResultLineage: {
                urn: 'urn:li:dataset:(urn:li:dataPlatform:snowflake,other.schema.table,PROD)',
                upstream: { filtered: 0, total: 9 },
                downstream: { filtered: 0, total: 9 },
            },
        });

        expect(await screen.findByText(textContent('Depends on 2 assets'))).toBeInTheDocument();
        expect(operations.filter((operation) => operation.toLowerCase().includes('lineage'))).toEqual([
            'getLineageCounts',
        ]);
    });

    it('keeps the detailed lineage query outside search summary', async () => {
        const operations: string[] = [];
        renderSection({ contextType: TabContextType.PROFILE_SIDEBAR, operations });

        await waitFor(() =>
            expect(operations.filter((operation) => operation.toLowerCase().includes('lineage'))).toEqual([
                'getSearchAcrossLineageCounts',
            ]),
        );
    });
});
