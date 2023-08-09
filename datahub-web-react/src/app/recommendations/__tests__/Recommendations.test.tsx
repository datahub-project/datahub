import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';
import { Route } from 'react-router';
import { HomePage } from '../../home/HomePage';
import { mocks } from '../../../Mocks';
import TestPageContainer from '../../../utils/test-utils/TestPageContainer';
import { PageRoutes } from '../../../conf/Global';
import { SearchPage } from '../../search/SearchPage';

describe('Recommendations', () => {
    it('home renders recommendations', async () => {
        const { getByText } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer>
                    <HomePage />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('Datasets')).toBeInTheDocument(),{timeout:10000});
        await waitFor(() => expect(getByText('Explore your data')).toBeInTheDocument(),{timeout:10000});
        // await waitFor(() => expect(getByText('Platforms')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Snowflake')).toBeInTheDocument(),{timeout:10000});
        // await waitFor(() => expect(getByText('Top Tags')).toBeInTheDocument());
        await waitFor(() => expect(getByText('TestTag')).toBeInTheDocument(),{timeout:10000});
        // await waitFor(() => expect(getByText('Most Popular')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Some Other Dataset')).toBeInTheDocument(),{timeout:10000});
    });

    it('search results renders recommendations', async () => {
        const { getByText } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer initialEntries={['/search?page=1&query=noresults']}>
                    <Route path={PageRoutes.SEARCH_RESULTS} render={() => <SearchPage />} />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(getByText('More you may be interested in')).toBeInTheDocument(),{timeout:10000});
        await waitFor(() => expect(getByText('Top Platforms')).toBeInTheDocument(),{timeout:10000});
        await waitFor(() => expect(getByText('Snowflake')).toBeInTheDocument(),{timeout:10000});
        await waitFor(() => expect(getByText('Popular Tags')).toBeInTheDocument(),{timeout:10000});
        await waitFor(() => expect(getByText('TestTag')).toBeInTheDocument(),{timeout:10000});
        await waitFor(() => expect(getByText('Most Popular')).toBeInTheDocument(),{timeout:10000});
        await waitFor(() => expect(getByText('Some Other Dataset')).toBeInTheDocument(),{timeout:10000});
    });

    // TODO: Uncomment once entity sidebar recs are fully supported.
    // it('renders entity page sidebar recommendations', async () => {
    //     const { getByText } = render(
    //         <MockedProvider
    //             mocks={mocks}
    //             addTypename={false}
    //             defaultOptions={{
    //                 watchQuery: { fetchPolicy: 'no-cache' },
    //                 query: { fetchPolicy: 'no-cache' },
    //             }}
    //         >
    //             <TestPageContainer initialEntries={['/dataset/urn:li:dataset:3']}>
    //                 <EntityProfile
    //                     urn="urn:li:dataset:3"
    //                     entityType={EntityType.Dataset}
    //                     useEntityQuery={useGetDatasetQuery}
    //                     useUpdateQuery={useUpdateDatasetMutation}
    //                     getOverrideProperties={() => ({})}
    //                     tabs={[
    //                         {
    //                             name: 'Schema',
    //                             component: SchemaTab,
    //                         },
    //                     ]}
    //                     sidebarSections={[
    //                         {
    //                             component: SidebarRecommendationsSection,
    //                         },
    //                     ]}
    //                 />
    //             </TestPageContainer>
    //         </MockedProvider>,
    //     );

    //     // find recommendation modules
    //     await waitFor(() => expect(getByText('Top Platforms')).toBeInTheDocument());
    //     await waitFor(() => expect(getByText('Snowflake')).toBeInTheDocument());
    //     await waitFor(() => expect(getByText('Popular Tags')).toBeInTheDocument());
    //     await waitFor(() => expect(getByText('TestTag')).toBeInTheDocument());
    //     await waitFor(() => expect(getByText('Most Popular')).toBeInTheDocument());
    //     await waitFor(() => expect(getByText('Some Other Dataset')).toBeInTheDocument());
    // });
});
