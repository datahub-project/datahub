import { ApolloLink, InMemoryCache, Observable } from '@apollo/client';
import { MockedProvider } from '@apollo/client/testing';
import { render, screen, waitFor } from '@testing-library/react';
import React from 'react';

import { EntityContext } from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import SidebarLineageSection from '@app/entityV2/shared/containers/profile/sidebar/Lineage/SidebarLineageSection';
import { TabContextType } from '@app/entityV2/shared/types';
import possibleTypesResult from '@src/possibleTypes.generated';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

import { EntityType } from '@types';

const URN = 'urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.my_schema.events,PROD)';

const entityData: GenericEntityProperties = {
    urn: URN,
    type: EntityType.Dataset,
};

function renderSection(contextType: TabContextType, operations: string[]) {
    const cache = new InMemoryCache({
        possibleTypes: possibleTypesResult.possibleTypes,
    });
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
            </TestPageContainer>
        </MockedProvider>,
    );
}

describe('SidebarLineageSection', () => {
    it('uses the lightweight relationship query for search summary counts', async () => {
        const operations: string[] = [];
        renderSection(TabContextType.SEARCH_SIDEBAR, operations);

        expect(await screen.findByText('UPSTREAM')).toBeInTheDocument();
        expect(screen.getByText('DOWNSTREAM')).toBeInTheDocument();
        expect(screen.getByText('2')).toBeInTheDocument();
        expect(screen.getByText('4')).toBeInTheDocument();
        expect(operations.filter((operation) => operation.toLowerCase().includes('lineage'))).toEqual([
            'getLineageCounts',
        ]);
    });

    it('keeps the detailed lineage query outside search summary', async () => {
        const operations: string[] = [];
        renderSection(TabContextType.PROFILE_SIDEBAR, operations);

        await waitFor(() =>
            expect(operations.filter((operation) => operation.toLowerCase().includes('lineage'))).toEqual([
                'getSearchAcrossLineageCounts',
            ]),
        );
    });
});
