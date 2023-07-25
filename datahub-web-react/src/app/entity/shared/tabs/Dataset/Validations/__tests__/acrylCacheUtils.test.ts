import { ApolloClient } from '@apollo/client';
import { updateDatasetAssertionsCache, removeFromDatasetAssertionsCache } from '../acrylCacheUtils';

describe('test cache add, update, removal', () => {
    const mockAssertion = { urn: 'test:assertion:urn', field: 'value' };
    const mockApolloClient = {
        readQuery: jest.fn(),
        writeQuery: jest.fn(),
    } as unknown as ApolloClient<unknown>;

    beforeEach(() => {
        // Clear all mocks before each test
        (mockApolloClient.readQuery as any).mockClear();
        (mockApolloClient.writeQuery as any).mockClear();
    });

    it('adds to dataset assertions cache', () => {
        // Mock readQuery function to return a predefined result
        (mockApolloClient.readQuery as any).mockReturnValue({
            dataset: {
                assertions: {
                    assertions: [],
                    count: 0,
                    total: 0,
                    start: 0,
                },
            },
        });

        // Adding new assertion.
        updateDatasetAssertionsCache('test:dataset:urn', mockAssertion, mockApolloClient);

        // Check if readQuery has been called once
        expect(mockApolloClient.readQuery).toHaveBeenCalledTimes(1);

        // Check if writeQuery has been called once
        expect(mockApolloClient.writeQuery).toHaveBeenCalledTimes(1);

        // Check if writeQuery has been called with correct data
        const writeQueryCallArg = (mockApolloClient.writeQuery as any).mock.calls[0][0];
        expect(writeQueryCallArg).toMatchObject({
            data: {
                dataset: {
                    assertions: {
                        assertions: [mockAssertion],
                        start: 0,
                        count: 1,
                        total: 1,
                    },
                },
            },
        });
    });

    it('updates dataset assertions cache', () => {
        // Mock readQuery function to return a predefined result
        (mockApolloClient.readQuery as any).mockReturnValue({
            dataset: {
                assertions: {
                    assertions: [mockAssertion],
                    count: 1,
                    total: 1,
                    start: 0,
                },
            },
        });

        // Adding new assertion.
        const modifiedMockAssertion = {
            ...mockAssertion,
            field: 'value2',
        };
        updateDatasetAssertionsCache('test:dataset:urn', modifiedMockAssertion, mockApolloClient);

        // Check if readQuery has been called once
        expect(mockApolloClient.readQuery).toHaveBeenCalledTimes(1);

        // Check if writeQuery has been called once
        expect(mockApolloClient.writeQuery).toHaveBeenCalledTimes(1);

        // Check if writeQuery has been called with correct data
        const writeQueryCallArg = (mockApolloClient.writeQuery as any).mock.calls[0][0];
        expect(writeQueryCallArg).toMatchObject({
            data: {
                dataset: {
                    assertions: {
                        assertions: [modifiedMockAssertion],
                        start: 0,
                        count: 1,
                        total: 1,
                    },
                },
            },
        });
    });

    it('removes from dataset assertions cache', () => {
        // Mock readQuery function to return a predefined result
        (mockApolloClient.readQuery as any).mockReturnValue({
            dataset: {
                assertions: {
                    assertions: [mockAssertion],
                    count: 1,
                    total: 1,
                    start: 0,
                },
            },
        });

        removeFromDatasetAssertionsCache('test:dataset:urn', 'test:assertion:urn', mockApolloClient);

        // Check if readQuery has been called once
        expect(mockApolloClient.readQuery).toHaveBeenCalledTimes(1);

        // Check if writeQuery has been called once
        expect(mockApolloClient.writeQuery).toHaveBeenCalledTimes(1);

        // Check if writeQuery has been called with correct data
        const writeQueryCallArg = (mockApolloClient.writeQuery as any).mock.calls[0][0];
        expect(writeQueryCallArg).toMatchObject({
            data: {
                dataset: {
                    assertions: {
                        assertions: [],
                        start: 0,
                        count: 0,
                        total: 0,
                    },
                },
            },
        });
    });
});
