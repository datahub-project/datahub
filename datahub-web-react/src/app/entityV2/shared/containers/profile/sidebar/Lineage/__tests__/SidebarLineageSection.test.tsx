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
const OTHER_URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.other,PROD)';

const defaultEntityData: GenericEntityProperties = {
    urn: URN,
    type: EntityType.Dataset,
};

function textContent(expected: string) {
    return (_: string, element: Element | null) => element?.textContent === expected;
}

function lineageResults(total: number, entityType?: string) {
    return {
        __typename: 'SearchAcrossLineageResults',
        start: 0,
        count: total,
        total,
        facets: entityType
            ? [
                  {
                      __typename: 'FacetMetadata',
                      field: '_entityType',
                      displayName: 'Type',
                      entity: null,
                      aggregations: [
                          { __typename: 'AggregationMetadata', value: entityType, count: total, entity: null },
                      ],
                  },
              ]
            : [],
    };
}

type RenderArgs = {
    contextType: TabContextType;
    operations: string[];
    searchResultLineage?: SearchResultLineageCounts | null;
    entityData?: GenericEntityProperties;
    cachedTypeBreakdown?: { upstreamTotal: number; downstreamTotal: number; withFacets: boolean };
};

function renderSection({
    contextType,
    operations,
    searchResultLineage,
    entityData = defaultEntityData,
    cachedTypeBreakdown,
}: RenderArgs) {
    const cache = new InMemoryCache({
        possibleTypes: possibleTypesResult.possibleTypes,
    });
    if (cachedTypeBreakdown) {
        const { upstreamTotal, downstreamTotal, withFacets } = cachedTypeBreakdown;
        cache.writeQuery({
            query: GetSearchAcrossLineageCountsDocument,
            variables: { urn: URN, startTimeMillis: null },
            data: {
                upstreams: lineageResults(upstreamTotal, withFacets ? 'DATASET' : undefined),
                downstreams: lineageResults(downstreamTotal, withFacets ? 'CHART' : undefined),
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
                        data: { upstreams: lineageResults(0), downstreams: lineageResults(0) },
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

function lineageOperations(operations: string[]) {
    return operations.filter((operation) => operation.toLowerCase().includes('lineage'));
}

describe('SidebarLineageSection', () => {
    it('falls back to generic asset counts without a cached type breakdown', async () => {
        const operations: string[] = [];
        renderSection({ contextType: TabContextType.SEARCH_SIDEBAR, operations });

        expect(await screen.findByText('UPSTREAM')).toBeInTheDocument();
        expect(screen.getByText('DOWNSTREAM')).toBeInTheDocument();
        expect(screen.getByText(textContent('Depends on 2 assets'))).toBeInTheDocument();
        expect(screen.getByText(textContent('Used by 4 assets'))).toBeInTheDocument();
        expect(lineageOperations(operations)).toEqual(['getLineageCounts']);
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
        expect(lineageOperations(operations)).toEqual([]);
    });

    it('names neighbors when the cached breakdown matches the counts', async () => {
        const operations: string[] = [];
        renderSection({
            contextType: TabContextType.SEARCH_SIDEBAR,
            operations,
            searchResultLineage: {
                urn: URN,
                upstream: { filtered: 0, total: 2 },
                downstream: { filtered: 0, total: 1 },
            },
            cachedTypeBreakdown: { upstreamTotal: 2, downstreamTotal: 1, withFacets: true },
        });

        expect(await screen.findByText(textContent('Depends on 2 datasets'))).toBeInTheDocument();
        expect(screen.getByText(textContent('Used by 1 chart'))).toBeInTheDocument();
        expect(lineageOperations(operations)).toEqual([]);
    });

    it('ignores a cached breakdown that disagrees with the current counts', async () => {
        const operations: string[] = [];
        renderSection({
            contextType: TabContextType.SEARCH_SIDEBAR,
            operations,
            searchResultLineage: {
                urn: URN,
                upstream: { filtered: 0, total: 5 },
                downstream: { filtered: 0, total: 1 },
            },
            cachedTypeBreakdown: { upstreamTotal: 2, downstreamTotal: 1, withFacets: true },
        });

        expect(await screen.findByText(textContent('Depends on 5 assets'))).toBeInTheDocument();
        expect(screen.getByText(textContent('Used by 1 chart'))).toBeInTheDocument();
        expect(lineageOperations(operations)).toEqual([]);
    });

    it('ignores an empty cached breakdown and still loads counts', async () => {
        const operations: string[] = [];
        renderSection({
            contextType: TabContextType.SEARCH_SIDEBAR,
            operations,
            cachedTypeBreakdown: { upstreamTotal: 0, downstreamTotal: 0, withFacets: false },
        });

        expect(await screen.findByText(textContent('Depends on 2 assets'))).toBeInTheDocument();
        expect(screen.getByText(textContent('Used by 4 assets'))).toBeInTheDocument();
        expect(lineageOperations(operations)).toEqual(['getLineageCounts']);
    });

    it('ignores search-result lineage that belongs to another urn', async () => {
        const operations: string[] = [];
        renderSection({
            contextType: TabContextType.SEARCH_SIDEBAR,
            operations,
            searchResultLineage: {
                urn: OTHER_URN,
                upstream: { filtered: 0, total: 9 },
                downstream: { filtered: 0, total: 9 },
            },
        });

        expect(await screen.findByText(textContent('Depends on 2 assets'))).toBeInTheDocument();
        expect(lineageOperations(operations)).toEqual(['getLineageCounts']);
    });

    it('renders nothing for combined sibling entities', async () => {
        const operations: string[] = [];
        renderSection({
            contextType: TabContextType.SEARCH_SIDEBAR,
            operations,
            entityData: { ...defaultEntityData, siblingsSearch: { total: 1, count: 1, searchResults: [] } },
            searchResultLineage: {
                urn: URN,
                upstream: { filtered: 0, total: 4 },
                downstream: { filtered: 0, total: 2 },
            },
        });

        await waitFor(() => expect(lineageOperations(operations)).toEqual([]));
        expect(screen.queryByText('UPSTREAM')).not.toBeInTheDocument();
        expect(screen.queryByText('DOWNSTREAM')).not.toBeInTheDocument();
    });

    it('keeps the detailed lineage query outside search summary', async () => {
        const operations: string[] = [];
        renderSection({ contextType: TabContextType.PROFILE_SIDEBAR, operations });

        await waitFor(() => expect(lineageOperations(operations)).toEqual(['getSearchAcrossLineageCounts']));
    });
});
