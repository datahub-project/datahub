import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { describe, expect, it } from 'vitest';

import { SearchDocumentsInput, useSearchDocuments } from '@app/document/hooks/useSearchDocuments';

import { SearchDocumentsDocument } from '@graphql/document.generated';
import { DocumentSourceType, DocumentState } from '@types';

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

    it('should search documents with Native source type', async () => {
        const input: SearchDocumentsInput = {
            fetchPolicy: 'network-only',
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'NATIVE',
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
            fetchPolicy: 'network-only',
            query: 'test query',
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'NATIVE',
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
            fetchPolicy: 'network-only',
            parentDocument: 'urn:li:document:parent',
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: ['urn:li:document:parent'],
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'NATIVE',
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
            fetchPolicy: 'network-only',
            rootOnly: true,
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: true,
                            types: undefined,
                            sourceType: 'NATIVE',
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
            fetchPolicy: 'network-only',
            types: ['guide', 'tutorial'],
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: ['guide', 'tutorial'],
                            sourceType: 'NATIVE',
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
            fetchPolicy: 'network-only',
            states: [DocumentState.Published],
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'NATIVE',
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

    it('should search all documents when both source types specified', async () => {
        const input: SearchDocumentsInput = {
            fetchPolicy: 'network-only',
            sourceTypes: [DocumentSourceType.Native, DocumentSourceType.External],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: undefined,
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
            fetchPolicy: 'network-only',
            start: 10,
            count: 50,
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'NATIVE',
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
            fetchPolicy: 'network-only',
            includeParentDocuments: true,
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'NATIVE',
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
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'NATIVE',
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
            fetchPolicy: 'network-only',
            query: 'nonexistent',
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'NATIVE',
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
        const input: SearchDocumentsInput = {
            fetchPolicy: 'network-only',
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'NATIVE',
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
        const input: SearchDocumentsInput = {
            fetchPolicy: 'network-only',
            sourceTypes: [DocumentSourceType.Native],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'NATIVE',
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

    it('should search External documents only', async () => {
        const input: SearchDocumentsInput = {
            fetchPolicy: 'network-only',
            sourceTypes: [DocumentSourceType.External],
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
                            parentDocuments: undefined,
                            rootOnly: undefined,
                            types: undefined,
                            sourceType: 'EXTERNAL',
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
        expect(result.current.total).toBe(2);
    });
});
