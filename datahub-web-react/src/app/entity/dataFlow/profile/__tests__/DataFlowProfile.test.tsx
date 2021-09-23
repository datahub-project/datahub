import React from 'react';
import { act } from 'react-dom/test-utils';
import { render, waitFor, fireEvent } from '@testing-library/react';
import { MockedProvider } from '@apollo/client/testing';

import { DataFlowProfile } from '../DataFlowProfile';
import TestPageContainer from '../../../../../utils/test-utils/TestPageContainer';
import { mocks } from '../../../../../Mocks';

describe('DataJobProfile', () => {
    it('renders', async () => {
        const promise = Promise.resolve();
        const { getByText, queryAllByText } = render(
            <MockedProvider mocks={mocks} addTypename={false}>
                <TestPageContainer initialEntries={['/pipelines/urn:li:dataFlow:1']}>
                    <DataFlowProfile urn="urn:li:dataFlow:1" />
                </TestPageContainer>
            </MockedProvider>,
        );
        await waitFor(() => expect(queryAllByText('DataFlowInfoName').length).toBeGreaterThanOrEqual(1));

        expect(getByText('DataFlowInfo1 Description')).toBeInTheDocument();
        await act(() => promise);
    });

    it('topological sort', async () => {
        const promise = Promise.resolve();
        const { getByTestId, getByText, queryAllByText, getAllByTestId } = render(
            <MockedProvider
                mocks={mocks}
                addTypename={false}
                defaultOptions={{
                    watchQuery: { fetchPolicy: 'no-cache' },
                    query: { fetchPolicy: 'no-cache' },
                }}
            >
                <TestPageContainer initialEntries={['/pipelines/urn:li:dataFlow:1']}>
                    <DataFlowProfile urn="urn:li:dataFlow:1" />
                </TestPageContainer>
            </MockedProvider>,
        );

        await waitFor(() => expect(queryAllByText('DataFlowInfoName').length).toBeGreaterThanOrEqual(1));
        const rawButton = getByText('Task');
        act(() => {
            fireEvent.click(rawButton);
        });
        await waitFor(() => expect(getByTestId('dataflow-jobs-list')).toBeInTheDocument());
        await waitFor(() => expect(queryAllByText('DataJobInfoName3').length).toBeGreaterThanOrEqual(1));
        const jobsList = getAllByTestId('datajob-item-preview');

        expect(jobsList.length).toBe(3);
        expect(jobsList[0].innerHTML).toMatch(/DataJobInfoName/);
        expect(jobsList[1].innerHTML).toMatch(/DataJobInfoName2/);
        expect(jobsList[2].innerHTML).toMatch(/DataJobInfoName3/);
        await act(() => promise);
    });
});
