import { ApolloClient, ApolloProvider, InMemoryCache } from '@apollo/client';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DocumentTreeContext } from '@app/document/DocumentTreeContext';
import { useLoadDocumentTree } from '@app/document/hooks/useLoadDocumentTree';
import * as useSearchDocumentsModule from '@app/document/hooks/useSearchDocuments';

import { SearchDocumentsDocument } from '@graphql/document.generated';

vi.mock('../useSearchDocuments');

describe('useLoadDocumentTree', () => {
    let mockClient: ApolloClient<any>;
    const mockInitializeTree = vi.fn();
    const mockSetNodeChildren = vi.fn();
    const mockGetRootNodes = vi.fn();

    const mockContextValue = {
        nodes: new Map(),
        rootUrns: [],
        initializeTree: mockInitializeTree,
        setNodeChildren: mockSetNodeChildren,
        getRootNodes: mockGetRootNodes,
        getChildren: vi.fn(),
        addNode: vi.fn(),
        deleteNode: vi.fn(),
        updateNodeTitle: vi.fn(),
        moveNode: vi.fn(),
        getNode: vi.fn(),
        expandedUrns: new Set<string>(),
        setExpandedUrns: vi.fn(),
        toggleExpanded: vi.fn(),
        expandNode: vi.fn(),
        collapseNode: vi.fn(),
    };

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

        mockGetRootNodes.mockReturnValue([]);
    });

    it('should load and initialize tree with root documents', async () => {
        const mockRootDocuments = [
            {
                urn: 'urn:li:document:root1',
                info: {
                    title: 'Root Document 1',
                    created: { time: 2000 },
                    parentDocument: null,
                },
            },
            {
                urn: 'urn:li:document:root2',
                info: {
                    title: 'Root Document 2',
                    created: { time: 1000 },
                    parentDocument: null,
                },
            },
        ];

        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: mockRootDocuments as any,
            total: 2,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        mockClient.query = vi.fn().mockResolvedValue({
            data: {
                searchDocuments: {
                    documents: [],
                    total: 0,
                },
            },
        });

        // Note: useApolloClient is mocked via module mock in real implementation

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(mockInitializeTree).toHaveBeenCalled();
        });

        // Check that documents were sorted by creation time (DESC)
        const initializeCall = mockInitializeTree.mock.calls[0][0];
        expect(initializeCall[0].urn).toBe('urn:li:document:root1'); // Newer document first
        expect(initializeCall[1].urn).toBe('urn:li:document:root2');
    });

    it('should not initialize tree if already initialized', async () => {
        const mockRootDocuments = [
            {
                urn: 'urn:li:document:root1',
                info: {
                    title: 'Root Document 1',
                    created: { time: 1000 },
                },
            },
        ];

        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: mockRootDocuments as any,
            total: 1,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        // Tree already has nodes
        mockGetRootNodes.mockReturnValue([
            {
                urn: 'urn:li:document:existing',
                title: 'Existing',
                parentUrn: null,
                hasChildren: false,
                children: [],
            },
        ]);

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            // Should not initialize because tree already has nodes
            expect(mockInitializeTree).not.toHaveBeenCalled();
        });
    });

    it('should check which root documents have children', async () => {
        const mockRootDocuments = [
            {
                urn: 'urn:li:document:root1',
                info: {
                    title: 'Root Document 1',
                    created: { time: 1000 },
                },
            },
        ];

        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: mockRootDocuments as any,
            total: 1,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        const mockChildDocuments = [
            {
                urn: 'urn:li:document:child1',
                info: {
                    parentDocument: {
                        document: {
                            urn: 'urn:li:document:root1',
                        },
                    },
                },
            },
        ];

        mockClient.query = vi.fn().mockResolvedValue({
            data: {
                searchDocuments: {
                    documents: mockChildDocuments,
                    total: 1,
                },
            },
        });

        // Note: useApolloClient is mocked via module mock in real implementation

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(mockInitializeTree).toHaveBeenCalled();
        });

        const initializeCall = mockInitializeTree.mock.calls[0][0];
        expect(initializeCall[0].hasChildren).toBe(true);
    });

    it('should handle loading state correctly', () => {
        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: [],
            total: 0,
            loading: true,
            error: undefined,
            refetch: vi.fn(),
        });

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        expect(result.current.loading).toBe(true);
    });

    it('should expose checkForChildren function', () => {
        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: [],
            total: 0,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        expect(typeof result.current.checkForChildren).toBe('function');
    });

    it('should check for children correctly', async () => {
        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: [],
            total: 0,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        const urns = ['urn:li:document:1', 'urn:li:document:2'];
        const mockChildDocuments = [
            {
                urn: 'urn:li:document:child1',
                info: {
                    parentDocument: {
                        document: {
                            urn: 'urn:li:document:1',
                        },
                    },
                },
            },
        ];

        mockClient.query = vi.fn().mockResolvedValue({
            data: {
                searchDocuments: {
                    documents: mockChildDocuments,
                    total: 1,
                },
            },
        });

        // Note: useApolloClient is mocked via module mock in real implementation

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        const childrenMap = await result.current.checkForChildren(urns);

        expect(childrenMap).toEqual({
            'urn:li:document:1': true,
            'urn:li:document:2': false,
        });

        expect(mockClient.query).toHaveBeenCalledWith({
            query: SearchDocumentsDocument,
            variables: {
                input: {
                    query: '*',
                    parentDocuments: urns,
                    start: 0,
                    count: 200, // 2 * 100
                },
            },
            fetchPolicy: 'network-only',
        });
    });

    it('should return empty map for checkForChildren with empty array', async () => {
        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: [],
            total: 0,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        const childrenMap = await result.current.checkForChildren([]);

        expect(childrenMap).toEqual({});
    });

    it('should handle errors in checkForChildren', async () => {
        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: [],
            total: 0,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        mockClient.query = vi.fn().mockRejectedValue(new Error('Network error'));

        // Note: useApolloClient is mocked via module mock in real implementation

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        const childrenMap = await result.current.checkForChildren(['urn:li:document:1']);

        expect(childrenMap).toEqual({});
    });

    it('should expose loadChildren function', () => {
        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: [],
            total: 0,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        expect(typeof result.current.loadChildren).toBe('function');
    });

    it('should load children for a parent', async () => {
        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: [],
            total: 0,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        const parentUrn = 'urn:li:document:parent1';
        const mockChildDocuments = [
            {
                urn: 'urn:li:document:child1',
                info: {
                    title: 'Child 1',
                    created: { time: 2000 },
                },
            },
            {
                urn: 'urn:li:document:child2',
                info: {
                    title: 'Child 2',
                    created: { time: 1000 },
                },
            },
        ];

        let queryCount = 0;
        mockClient.query = vi.fn().mockImplementation(() => {
            queryCount++;
            // First call is to fetch children
            if (queryCount === 1) {
                return Promise.resolve({
                    data: { searchDocuments: { documents: mockChildDocuments, total: 2 } },
                });
            }
            // Second call is for checkForChildren (checking if those children have children)
            return Promise.resolve({
                data: { searchDocuments: { documents: [], total: 0 } },
            });
        });

        // Note: useApolloClient is mocked via module mock in real implementation

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        const children = await result.current.loadChildren(parentUrn);

        expect(children).toHaveLength(2);
        expect(children[0].urn).toBe('urn:li:document:child1'); // Sorted by time DESC
        expect(children[1].urn).toBe('urn:li:document:child2');
        expect(mockSetNodeChildren).toHaveBeenCalledWith(parentUrn, children);

        // Verify the first query call (loadChildren) matches the implementation
        const queryMock = vi.mocked(mockClient.query);
        const firstCall = queryMock.mock.calls[0];
        expect(firstCall[0]).toMatchObject({
            query: SearchDocumentsDocument,
            variables: {
                input: {
                    query: '*',
                    parentDocuments: [parentUrn],
                    start: 0,
                    count: 100,
                },
            },
            fetchPolicy: 'cache-first',
        });
    });

    it('should handle errors in loadChildren', async () => {
        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: [],
            total: 0,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        mockClient.query = vi.fn().mockRejectedValue(new Error('Network error'));

        // Note: useApolloClient is mocked via module mock in real implementation

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        const children = await result.current.loadChildren('urn:li:document:parent1');

        expect(children).toEqual([]);
    });

    it('should use "Untitled" as default title for documents without title', async () => {
        const mockRootDocuments = [
            {
                urn: 'urn:li:document:root1',
                info: {
                    title: null,
                    created: { time: 1000 },
                },
            },
        ];

        vi.mocked(useSearchDocumentsModule.useSearchDocuments).mockReturnValue({
            documents: mockRootDocuments as any,
            total: 1,
            loading: false,
            error: undefined,
            refetch: vi.fn(),
        });

        mockClient.query = vi.fn().mockResolvedValue({
            data: {
                searchDocuments: {
                    documents: [],
                    total: 0,
                },
            },
        });

        // Note: useApolloClient is mocked via module mock in real implementation

        const wrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
            </ApolloProvider>
        );

        renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(mockInitializeTree).toHaveBeenCalled();
        });

        const initializeCall = mockInitializeTree.mock.calls[0][0];
        expect(initializeCall[0].title).toBe('Untitled');
    });
});
