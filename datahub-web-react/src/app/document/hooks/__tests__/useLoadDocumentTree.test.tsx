import { ApolloClient, ApolloProvider, InMemoryCache } from '@apollo/client';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DEFAULT_STATE, UserContext } from '@app/context/userContext';
import { DocumentTreeContext } from '@app/document/DocumentTreeContext';
import { DOCUMENT_PAGE_SIZE, useLoadDocumentTree } from '@app/document/hooks/useLoadDocumentTree';

import * as documentGenerated from '@graphql/document.generated';

vi.mock('@graphql/document.generated');

describe('useLoadDocumentTree', () => {
    let mockClient: ApolloClient<any>;
    let mockSearchDocumentsLazyQuery: any;
    const mockInitializeTree = vi.fn();
    const mockAppendRootNodes = vi.fn();
    const mockSetNodeChildren = vi.fn();
    const mockAppendNodeChildren = vi.fn();
    const mockGetRootNodes = vi.fn();

    const mockContextValue = {
        nodes: new Map(),
        rootUrns: [],
        initializeTree: mockInitializeTree,
        appendRootNodes: mockAppendRootNodes,
        setNodeChildren: mockSetNodeChildren,
        appendNodeChildren: mockAppendNodeChildren,
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
        console.log = vi.fn();
        console.error = vi.fn();

        mockClient = new ApolloClient({
            cache: new InMemoryCache(),
            defaultOptions: { query: { fetchPolicy: 'no-cache' } },
        });

        mockGetRootNodes.mockReturnValue([]);

        mockSearchDocumentsLazyQuery = vi.fn().mockResolvedValue({
            data: { searchDocuments: { documents: [], total: 0 } },
        });

        vi.mocked(documentGenerated.useSearchDocumentsLazyQuery).mockReturnValue([
            mockSearchDocumentsLazyQuery,
            {} as any,
        ]);
    });

    const wrapper = ({ children }: any) => (
        <ApolloProvider client={mockClient}>
            <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
        </ApolloProvider>
    );

    it('should load first page of root documents and initialize tree', async () => {
        const mockDocs = [
            { urn: 'urn:li:document:root1', info: { title: 'Root 1', created: { time: 2000 }, parentDocument: null } },
            { urn: 'urn:li:document:root2', info: { title: 'Root 2', created: { time: 1000 }, parentDocument: null } },
        ];

        mockSearchDocumentsLazyQuery.mockResolvedValue({
            data: { searchDocuments: { documents: mockDocs, total: 2 } },
        });

        renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(mockInitializeTree).toHaveBeenCalled();
        });

        expect(mockSearchDocumentsLazyQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({
                        start: 0,
                        count: DOCUMENT_PAGE_SIZE,
                        rootOnly: true,
                    }),
                }),
            }),
        );

        const initializeCall = mockInitializeTree.mock.calls[0][0];
        expect(initializeCall[0].urn).toBe('urn:li:document:root1');
        expect(initializeCall[1].urn).toBe('urn:li:document:root2');
    });

    it('should not initialize tree if already populated', async () => {
        mockGetRootNodes.mockReturnValue([
            { urn: 'urn:li:document:existing', title: 'Existing', parentUrn: null, hasChildren: false },
        ]);

        mockSearchDocumentsLazyQuery.mockResolvedValue({
            data: {
                searchDocuments: {
                    documents: [{ urn: 'urn:li:document:root1', info: { title: 'Root 1', created: { time: 1000 } } }],
                    total: 1,
                },
            },
        });

        renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(mockSearchDocumentsLazyQuery).toHaveBeenCalled();
        });

        expect(mockInitializeTree).not.toHaveBeenCalled();
    });

    it('should report hasMoreRoots when total exceeds loaded count', async () => {
        const docs = Array.from({ length: DOCUMENT_PAGE_SIZE }, (_, i) => ({
            urn: `urn:li:document:root${i}`,
            info: { title: `Root ${i}`, created: { time: 1000 - i }, parentDocument: null },
        }));

        let callCount = 0;
        mockSearchDocumentsLazyQuery.mockImplementation(() => {
            callCount++;
            if (callCount === 1) {
                return Promise.resolve({
                    data: { searchDocuments: { documents: docs, total: 50 } },
                });
            }
            return Promise.resolve({
                data: { searchDocuments: { documents: [], total: 0 } },
            });
        });

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(mockInitializeTree).toHaveBeenCalled();
        });

        expect(result.current.hasMoreRoots).toBe(true);
    });

    it('should report hasMoreRoots=false when all loaded', async () => {
        const docs = [
            { urn: 'urn:li:document:root1', info: { title: 'Root 1', created: { time: 1000 }, parentDocument: null } },
        ];

        mockSearchDocumentsLazyQuery.mockResolvedValue({
            data: { searchDocuments: { documents: docs, total: 1 } },
        });

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(mockInitializeTree).toHaveBeenCalled();
        });

        expect(result.current.hasMoreRoots).toBe(false);
    });

    it('should load children with pagination tracking', async () => {
        const parentUrn = 'urn:li:document:parent1';
        const mockChildren = [
            { urn: 'urn:li:document:child1', info: { title: 'Child 1', created: { time: 2000 } } },
            { urn: 'urn:li:document:child2', info: { title: 'Child 2', created: { time: 1000 } } },
        ];

        let queryCount = 0;
        mockSearchDocumentsLazyQuery.mockImplementation(() => {
            queryCount++;
            if (queryCount === 1) {
                // Initial root load (empty)
                return Promise.resolve({ data: { searchDocuments: { documents: [], total: 0 } } });
            }
            if (queryCount === 2) {
                // loadChildren call
                return Promise.resolve({
                    data: { searchDocuments: { documents: mockChildren, total: 10 } },
                });
            }
            // checkForChildren
            return Promise.resolve({ data: { searchDocuments: { documents: [], total: 0 } } });
        });

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        const children = await result.current.loadChildren(parentUrn);

        expect(children).toHaveLength(2);
        expect(mockSetNodeChildren).toHaveBeenCalledWith(parentUrn, children);
        expect(result.current.hasMoreChildren(parentUrn)).toBe(true);
    });

    it('should handle checkForChildren correctly', async () => {
        const urns = ['urn:li:document:1', 'urn:li:document:2'];
        const mockChildDocs = [
            { urn: 'urn:li:document:child1', info: { parentDocument: { document: { urn: 'urn:li:document:1' } } } },
        ];

        let callCount = 0;
        mockSearchDocumentsLazyQuery.mockImplementation(() => {
            callCount++;
            if (callCount === 1) {
                return Promise.resolve({ data: { searchDocuments: { documents: [], total: 0 } } });
            }
            return Promise.resolve({ data: { searchDocuments: { documents: mockChildDocs, total: 1 } } });
        });

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        const childrenMap = await result.current.checkForChildren(urns);

        expect(childrenMap).toEqual({
            'urn:li:document:1': true,
            'urn:li:document:2': false,
        });
    });

    it('should return empty map for checkForChildren with empty array', async () => {
        mockSearchDocumentsLazyQuery.mockResolvedValue({
            data: { searchDocuments: { documents: [], total: 0 } },
        });

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });

        const childrenMap = await result.current.checkForChildren([]);
        expect(childrenMap).toEqual({});
    });

    it('should handle errors gracefully', async () => {
        mockSearchDocumentsLazyQuery.mockRejectedValue(new Error('Network error'));

        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        await waitFor(() => {
            expect(result.current.loading).toBe(false);
        });
    });

    it('should handle loading state correctly', () => {
        // isInitializing starts true, so loading is true on first render
        const { result } = renderHook(() => useLoadDocumentTree(), { wrapper });

        expect(result.current.loading).toBe(true);
    });

    it('should scope tree queries to the active View', async () => {
        const viewUrn = 'urn:li:dataHubView:test';
        const viewWrapper = ({ children }: any) => (
            <ApolloProvider client={mockClient}>
                <UserContext.Provider
                    value={{
                        loaded: true,
                        urn: 'urn:li:corpuser:test',
                        localState: { selectedViewUrn: viewUrn },
                        state: DEFAULT_STATE,
                        updateLocalState: () => null,
                        updateState: () => null,
                        refetchUser: () => null,
                    }}
                >
                    <DocumentTreeContext.Provider value={mockContextValue}>{children}</DocumentTreeContext.Provider>
                </UserContext.Provider>
            </ApolloProvider>
        );

        renderHook(() => useLoadDocumentTree(), { wrapper: viewWrapper });

        await waitFor(() => {
            expect(mockSearchDocumentsLazyQuery).toHaveBeenCalled();
        });

        expect(mockSearchDocumentsLazyQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                variables: expect.objectContaining({
                    input: expect.objectContaining({ viewUrn }),
                }),
            }),
        );
    });
});
