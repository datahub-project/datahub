import { MockedProvider } from '@apollo/client/testing';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import React from 'react';
import { MemoryRouter } from 'react-router-dom';

import { SetApplicationModal } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SetApplicationModal';
import CustomThemeProvider from '@src/CustomThemeProvider';

import { useBatchSetApplicationMutation, useGetApplicationsListLazyQuery } from '@graphql/application.generated';

vi.mock('@graphql/application.generated');

const mockUseGetApplicationsListLazyQuery = vi.mocked(useGetApplicationsListLazyQuery);
const mockUseBatchSetApplicationMutation = vi.mocked(useBatchSetApplicationMutation);

const mockGetApplications = vi.fn();
const mockBatchSetApplication = vi.fn();

const makeSearchResult = (urn: string, name: string) => ({
    entity: {
        __typename: 'Application' as const,
        urn,
        properties: { name, description: null, numAssets: 0 },
        domain: null,
    },
});

const defaultQueryResult = {
    data: {
        searchAcrossEntities: {
            searchResults: [
                makeSearchResult('urn:li:application:1', 'App Alpha'),
                makeSearchResult('urn:li:application:2', 'App Beta'),
            ],
        },
    },
    loading: false,
    error: undefined,
    called: true,
    client: {} as any,
    observable: {} as any,
    networkStatus: 7,
    fetchMore: vi.fn(),
    refetch: vi.fn(),
    reobserve: vi.fn(),
    subscribeToMore: vi.fn(),
    updateQuery: vi.fn(),
    startPolling: vi.fn(),
    stopPolling: vi.fn(),
    variables: undefined,
};

const renderModal = (props?: Partial<React.ComponentProps<typeof SetApplicationModal>>) => {
    return render(
        <MockedProvider mocks={[]} addTypename={false}>
            <CustomThemeProvider>
                <MemoryRouter>
                    <SetApplicationModal urns={['urn:li:dataset:1']} onCloseModal={vi.fn()} {...props} />
                </MemoryRouter>
            </CustomThemeProvider>
        </MockedProvider>,
    );
};

describe('SetApplicationModal', () => {
    beforeEach(() => {
        vi.clearAllMocks();

        mockUseGetApplicationsListLazyQuery.mockReturnValue([mockGetApplications, defaultQueryResult as any]);
        mockUseBatchSetApplicationMutation.mockReturnValue([mockBatchSetApplication, {} as any]);
    });

    it('renders the modal with title', () => {
        renderModal();
        expect(screen.getByText('Set Application')).toBeInTheDocument();
    });

    it('fires an initial query on mount with wildcard to populate the dropdown', () => {
        renderModal();

        expect(mockGetApplications).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        query: '*',
                        types: ['APPLICATION'],
                    }),
                }),
            }),
        );
    });

    it('calls server search with typed query when user types in the select', async () => {
        renderModal();

        const select = screen.getByRole('combobox');
        fireEvent.change(select, { target: { value: 'Alpha' } });

        await waitFor(
            () => {
                expect(mockGetApplications).toHaveBeenCalledWith(
                    expect.objectContaining({
                        variables: expect.objectContaining({
                            input: expect.objectContaining({
                                query: 'Alpha',
                                types: ['APPLICATION'],
                            }),
                        }),
                    }),
                );
            },
            { timeout: 2000 },
        );
    });

    it('shows error message when query fails', () => {
        mockUseGetApplicationsListLazyQuery.mockReturnValue([
            mockGetApplications,
            {
                ...defaultQueryResult,
                data: undefined,
                error: new Error('Network error') as any,
            } as any,
        ]);

        renderModal();
        expect(screen.getByText(/Failed to load applications/)).toBeInTheDocument();
    });

    it('calls batchSetApplication mutation with selected application urn on OK', async () => {
        mockBatchSetApplication.mockResolvedValue({});
        renderModal();

        const select = screen.getByRole('combobox');
        fireEvent.mouseDown(select);

        await waitFor(() => screen.getByText('App Alpha'));
        fireEvent.click(screen.getByText('App Alpha'));

        const okButton = screen.getByText('OK');
        fireEvent.click(okButton);

        await waitFor(() => {
            expect(mockBatchSetApplication).toHaveBeenCalledWith(
                expect.objectContaining({
                    variables: expect.objectContaining({
                        input: expect.objectContaining({
                            applicationUrn: 'urn:li:application:1',
                            resourceUrns: ['urn:li:dataset:1'],
                        }),
                    }),
                }),
            );
        });
    });

    it('does not call mutation when no application is selected', () => {
        renderModal();

        const okButton = screen.getByText('OK');
        fireEvent.click(okButton);

        expect(mockBatchSetApplication).not.toHaveBeenCalled();
    });
});
