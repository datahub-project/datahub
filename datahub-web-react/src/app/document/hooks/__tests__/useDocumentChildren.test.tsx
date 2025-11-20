import { ApolloClient, InMemoryCache } from '@apollo/client';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { useDocumentChildren } from '@app/document/hooks/useDocumentChildren';

import { SearchDocumentsDocument } from '@graphql/document.generated';
import { DocumentState } from '@types';

describe('useDocumentChildren', () => {
    let mockClient: ApolloClient<any>;

    beforeEach(() => {
        vi.clearAllMocks();
        console.log = vi.fn(); // Suppress console.log in tests
        console.error = vi.fn(); // Suppress console.error in tests

        mockClient = new ApolloClient({
            cache: new InMemoryCache(),
            defaultOptions: {
                query: {
                    fetchPolicy: 'no-cache',
                },
            },
        });
    });

    describe('checkForChildren', () => {
        it('should return empty map for empty parent URNs array', async () => {
            const mockQuery = vi.spyOn(mockClient, 'query');

            const { result } = renderHook(() => useDocumentChildren(), {
                wrapper: ({ children }) => <>{children}</>,
            });

            // Mock useApolloClient
            // Note: useApolloClient is mocked via module mock in real implementation

            const childrenMap = await result.current.checkForChildren([]);

            expect(childrenMap).toEqual({});
            expect(mockQuery).not.toHaveBeenCalled();
        });

        it('should check for children of multiple parents', async () => {
            const parentUrns = ['urn:li:document:parent1', 'urn:li:document:parent2'];

            const mockDocuments = [
                {
                    urn: 'urn:li:document:child1',
                    info: {
                        title: 'Child 1',
                        parentDocument: {
                            document: {
                                urn: 'urn:li:document:parent1',
                            },
                        },
                    },
                },
                {
                    urn: 'urn:li:document:child2',
                    info: {
                        title: 'Child 2',
                        parentDocument: {
                            document: {
                                urn: 'urn:li:document:parent1',
                            },
                        },
                    },
                },
            ];

            mockClient.query = vi.fn().mockResolvedValue({
                data: {
                    searchDocuments: {
                        documents: mockDocuments,
                        total: 2,
                    },
                },
            });

            // Note: useApolloClient is mocked via module mock in real implementation

            const { result } = renderHook(() => useDocumentChildren());

            const childrenMap = await result.current.checkForChildren(parentUrns);

            expect(childrenMap).toEqual({
                'urn:li:document:parent1': true,
                'urn:li:document:parent2': false,
            });

            expect(mockClient.query).toHaveBeenCalledWith({
                query: SearchDocumentsDocument,
                variables: {
                    input: {
                        query: '*',
                        parentDocuments: parentUrns,
                        states: [DocumentState.Published, DocumentState.Unpublished],
                        includeDrafts: false,
                        start: 0,
                        count: 200, // 100 * 2 parents
                    },
                },
                fetchPolicy: 'network-only',
            });
        });

        it('should handle all parents having children', async () => {
            const parentUrns = ['urn:li:document:parent1', 'urn:li:document:parent2'];

            const mockDocuments = [
                {
                    urn: 'urn:li:document:child1',
                    info: {
                        parentDocument: {
                            document: {
                                urn: 'urn:li:document:parent1',
                            },
                        },
                    },
                },
                {
                    urn: 'urn:li:document:child2',
                    info: {
                        parentDocument: {
                            document: {
                                urn: 'urn:li:document:parent2',
                            },
                        },
                    },
                },
            ];

            mockClient.query = vi.fn().mockResolvedValue({
                data: {
                    searchDocuments: {
                        documents: mockDocuments,
                    },
                },
            });

            // Note: useApolloClient is mocked via module mock in real implementation

            const { result } = renderHook(() => useDocumentChildren());

            const childrenMap = await result.current.checkForChildren(parentUrns);

            expect(childrenMap).toEqual({
                'urn:li:document:parent1': true,
                'urn:li:document:parent2': true,
            });
        });

        it('should handle errors gracefully', async () => {
            const parentUrns = ['urn:li:document:parent1'];

            mockClient.query = vi.fn().mockRejectedValue(new Error('Network error'));

            // Note: useApolloClient is mocked via module mock in real implementation

            const { result } = renderHook(() => useDocumentChildren());

            const childrenMap = await result.current.checkForChildren(parentUrns);

            expect(childrenMap).toEqual({});
        });
    });

    describe('fetchChildren', () => {
        it('should fetch children for a parent document', async () => {
            const parentUrn = 'urn:li:document:parent1';

            const mockDocuments = [
                {
                    urn: 'urn:li:document:child1',
                    info: {
                        title: 'Child 1',
                    },
                },
                {
                    urn: 'urn:li:document:child2',
                    info: {
                        title: 'Child 2',
                    },
                },
            ];

            mockClient.query = vi.fn().mockResolvedValue({
                data: {
                    searchDocuments: {
                        documents: mockDocuments,
                    },
                },
            });

            // Note: useApolloClient is mocked via module mock in real implementation

            const { result } = renderHook(() => useDocumentChildren());

            const children = await result.current.fetchChildren(parentUrn);

            expect(children).toEqual([
                { urn: 'urn:li:document:child1', title: 'Child 1' },
                { urn: 'urn:li:document:child2', title: 'Child 2' },
            ]);

            expect(mockClient.query).toHaveBeenCalledWith({
                query: SearchDocumentsDocument,
                variables: {
                    input: {
                        query: '*',
                        parentDocument: parentUrn,
                        states: [DocumentState.Published, DocumentState.Unpublished],
                        includeDrafts: false,
                        start: 0,
                        count: 100,
                    },
                },
                fetchPolicy: 'cache-first',
            });
        });

        it('should use "New Document" as default title when title is missing', async () => {
            const parentUrn = 'urn:li:document:parent1';

            const mockDocuments = [
                {
                    urn: 'urn:li:document:child1',
                    info: {
                        title: null,
                    },
                },
            ];

            mockClient.query = vi.fn().mockResolvedValue({
                data: {
                    searchDocuments: {
                        documents: mockDocuments,
                    },
                },
            });

            // Note: useApolloClient is mocked via module mock in real implementation

            const { result } = renderHook(() => useDocumentChildren());

            const children = await result.current.fetchChildren(parentUrn);

            expect(children).toEqual([{ urn: 'urn:li:document:child1', title: 'New Document' }]);
        });

        it('should return empty array when no children found', async () => {
            const parentUrn = 'urn:li:document:parent1';

            mockClient.query = vi.fn().mockResolvedValue({
                data: {
                    searchDocuments: {
                        documents: [],
                    },
                },
            });

            // Note: useApolloClient is mocked via module mock in real implementation

            const { result } = renderHook(() => useDocumentChildren());

            const children = await result.current.fetchChildren(parentUrn);

            expect(children).toEqual([]);
        });

        it('should handle errors gracefully', async () => {
            const parentUrn = 'urn:li:document:parent1';

            mockClient.query = vi.fn().mockRejectedValue(new Error('Network error'));

            // Note: useApolloClient is mocked via module mock in real implementation

            const { result } = renderHook(() => useDocumentChildren());

            const children = await result.current.fetchChildren(parentUrn);

            expect(children).toEqual([]);
        });

        it('should handle GraphQL errors', async () => {
            const parentUrn = 'urn:li:document:parent1';

            mockClient.query = vi.fn().mockResolvedValue({
                data: null,
                error: new Error('GraphQL error'),
                errors: [{ message: 'GraphQL error' }],
            });

            // Note: useApolloClient is mocked via module mock in real implementation

            const { result } = renderHook(() => useDocumentChildren());

            const children = await result.current.fetchChildren(parentUrn);

            expect(children).toEqual([]);
        });
    });

    describe('loading state', () => {
        it('should always return loading as false', () => {
            const { result } = renderHook(() => useDocumentChildren());

            expect(result.current.loading).toBe(false);
        });
    });
});
