/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { MockedProvider } from '@apollo/client/testing';
import { render, waitFor } from '@testing-library/react';
import React from 'react';
import { Route } from 'react-router';

import { HomePage } from '@app/home/HomePage';
import { SearchPage } from '@app/search/SearchPage';
import { PageRoutes } from '@conf/Global';
import { mocks } from '@src/Mocks';
import TestPageContainer from '@utils/test-utils/TestPageContainer';

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
        await waitFor(() => expect(getByText('Datasets')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Explore your data')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Top Platforms')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Snowflake')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Popular Tags')).toBeInTheDocument());
        await waitFor(() => expect(getByText('TestTag')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Most Popular')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Some Other Dataset')).toBeInTheDocument());
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
        await waitFor(() => expect(getByText('More you may be interested in')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Top Platforms')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Snowflake')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Popular Tags')).toBeInTheDocument());
        await waitFor(() => expect(getByText('TestTag')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Most Popular')).toBeInTheDocument());
        await waitFor(() => expect(getByText('Some Other Dataset')).toBeInTheDocument());
    });

    // TODO: Uncomment once entity sidebar recs are fully supported.
    // eslint-disable-next-line vitest/no-commented-out-tests
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
