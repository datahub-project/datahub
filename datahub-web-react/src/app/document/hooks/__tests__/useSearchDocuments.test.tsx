import { MockedProvider } from '@apollo/client/testing';
import { renderHook } from '@testing-library/react-hooks';
import { waitFor } from '@testing-library/react';
import React from 'react';
import { describe, expect, it } from 'vitest';


import { SearchDocumentsDocument } from '@graphql/document.generated';
import { DocumentState } from '@types';
import { useSearchDocuments, SearchDocumentsInput } from '@app/document/hooks/useSearchDocuments';

describe('useSearchDocuments', () => {
    const mockDocuments = [
        {
            urn: 'urn:li:document:1',
            info: {
                title: 'Test Document 1',
                created: { time: 1000 },
            },
        },
        {
            urn: 'urn:li:document:2',
            info: {
                title: 'Test Document 2',
                created: { time: 2000 },
            },
        },
    ];

    it('should search documents with default parameters', async () => {
        const input: SearchDocumentsInput = {};

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: '*',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: mockDocuments,
                            total: 2,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        expect(result.current.loading).toBe(true);

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual(mockDocuments);
        expect(result.current.total).toBe(2);
        expect(result.current.error).toBeUndefined();
    });

    it('should search documents with custom query', async () => {
        const input: SearchDocumentsInput = {
            query: 'test query',
        };

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: 'test query',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: [mockDocuments[0]],
                            total: 1,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual([mockDocuments[0]]);
        expect(result.current.total).toBe(1);
    });

    it('should search documents with parent document filter', async () => {
        const input: SearchDocumentsInput = {
            parentDocument: 'urn:li:document:parent',
        };

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: '*',
                            parentDocument: 'urn:li:document:parent',
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: mockDocuments,
                            total: 2,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual(mockDocuments);
    });

    it('should search root-only documents', async () => {
        const input: SearchDocumentsInput = {
            rootOnly: true,
        };

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: '*',
                            parentDocument: undefined,
                            rootOnly: true,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: [mockDocuments[0]],
                            total: 1,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual([mockDocuments[0]]);
    });

    it('should filter documents by types', async () => {
        const input: SearchDocumentsInput = {
            types: ['guide', 'tutorial'],
        };

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: '*',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: ['guide', 'tutorial'],
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: mockDocuments,
                            total: 2,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual(mockDocuments);
    });

    it('should filter documents by states', async () => {
        const input: SearchDocumentsInput = {
            states: [DocumentState.Published],
        };

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: '*',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: [mockDocuments[0]],
                            total: 1,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual([mockDocuments[0]]);
    });

    it('should include drafts when specified', async () => {
        const input: SearchDocumentsInput = {
            includeDrafts: true,
        };

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: '*',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: true,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: mockDocuments,
                            total: 2,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual(mockDocuments);
    });

    it('should handle pagination with start and count', async () => {
        const input: SearchDocumentsInput = {
            start: 10,
            count: 50,
        };

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 10,
                            count: 50,
                            query: '*',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: mockDocuments,
                            total: 100,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual(mockDocuments);
        expect(result.current.total).toBe(100);
    });

    it('should include parent documents when specified', async () => {
        const input: SearchDocumentsInput = {
            includeParentDocuments: true,
        };

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: '*',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: true,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: mockDocuments,
                            total: 2,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual(mockDocuments);
    });

    it('should use specified fetch policy', async () => {
        const input: SearchDocumentsInput = {
            fetchPolicy: 'network-only',
        };

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: '*',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: mockDocuments,
                            total: 2,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual(mockDocuments);
    });

    it('should return empty array when no documents found', async () => {
        const input: SearchDocumentsInput = {
            query: 'nonexistent',
        };

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: 'nonexistent',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: [],
                            total: 0,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.documents).toEqual([]);
        expect(result.current.total).toBe(0);
    });

    it('should handle GraphQL errors', async () => {
        const input: SearchDocumentsInput = {};

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: '*',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                error: new Error('Network error'),
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.error).toBeDefined();
        });

        expect(result.current.documents).toEqual([]);
        expect(result.current.total).toBe(0);
    });

    it('should expose refetch function', async () => {
        const input: SearchDocumentsInput = {};

        const mocks = [
            {
                request: {
                    query: SearchDocumentsDocument,
                    variables: {
                        input: {
                            start: 0,
                            count: 100,
                            query: '*',
                            parentDocument: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            states: [DocumentState.Published, DocumentState.Unpublished],
                            includeDrafts: false,
                        },
                        includeParentDocuments: false,
                    },
                },
                result: {
                    data: {
                        searchDocuments: {
                            documents: mockDocuments,
                            total: 2,
                        },
                    },
                },
            },
        ];

        const { result } = renderHook(() => useSearchDocuments(input), {
            wrapper: ({ children }) => (
                <MockedProvider mocks={mocks} addTypename={false}>
                    {children}
                </MockedProvider>
            ),
        });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        expect(result.current.refetch).toBeDefined();
        expect(typeof result.current.refetch).toBe('function');
    });
});

