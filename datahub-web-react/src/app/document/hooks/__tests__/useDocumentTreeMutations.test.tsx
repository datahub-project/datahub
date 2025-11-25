import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DocumentTreeContext } from '@app/document/DocumentTreeContext';
import {
    useCreateDocumentTreeMutation,
    useDeleteDocumentTreeMutation,
    useMoveDocumentTreeMutation,
    useUpdateDocumentTitleMutation,
} from '@app/document/hooks/useDocumentTreeMutations';

import {
    CreateDocumentDocument,
    DeleteDocumentDocument,
    MoveDocumentDocument,
    UpdateDocumentContentsDocument,
} from '@graphql/document.generated';

// Mock antd message
vi.mock('antd', () => ({
    message: {
        error: vi.fn(),
        success: vi.fn(),
    },
}));

describe('useDocumentTreeMutations', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        console.error = vi.fn(); // Suppress console.error in tests
    });

    describe('useCreateDocumentTreeMutation', () => {
        it('should create a document and update tree optimistically', async () => {
            const mockAddNode = vi.fn();
            const mockDeleteNode = vi.fn();

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: mockAddNode,
                deleteNode: mockDeleteNode,
                updateNodeTitle: vi.fn(),
                moveNode: vi.fn(),
                getNode: vi.fn(),
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const newUrn = 'urn:li:document:new123';
            const mocks = [
                {
                    request: {
                        query: CreateDocumentDocument,
                        variables: {
                            input: {
                                title: 'New Document',
                                parentDocument: null,
                                subType: undefined,
                                contents: { text: '' },
                            },
                        },
                    },
                    result: {
                        data: {
                            createDocument: newUrn,
                        },
                    },
                },
            ];

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useCreateDocumentTreeMutation(), { wrapper });

            const createdUrn = result.current.createDocument({
                title: 'New Document',
                parentDocument: null,
            });

            // Should add optimistic node immediately
            await waitFor(() => {
                expect(mockAddNode).toHaveBeenCalledWith(
                    expect.objectContaining({
                        title: 'New Document',
                        parentUrn: null,
                        hasChildren: false,
                    }),
                );
            });

            const urn = await createdUrn;

            // Should replace temp node with real node
            await waitFor(() => {
                expect(mockDeleteNode).toHaveBeenCalled();
                expect(mockAddNode).toHaveBeenCalledWith(
                    expect.objectContaining({
                        urn: newUrn,
                        title: 'New Document',
                        parentUrn: null,
                    }),
                );
            });

            expect(urn).toBe(newUrn);
        });

        it('should rollback on error', async () => {
            const mockAddNode = vi.fn();
            const mockDeleteNode = vi.fn();

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: mockAddNode,
                deleteNode: mockDeleteNode,
                updateNodeTitle: vi.fn(),
                moveNode: vi.fn(),
                getNode: vi.fn(),
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const mocks = [
                {
                    request: {
                        query: CreateDocumentDocument,
                        variables: {
                            input: {
                                title: 'Failed Document',
                                parentDocument: null,
                                subType: undefined,
                                contents: { text: '' },
                            },
                        },
                    },
                    error: new Error('Server error'),
                },
            ];

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useCreateDocumentTreeMutation(), { wrapper });

            const createdUrn = await result.current.createDocument({
                title: 'Failed Document',
                parentDocument: null,
            });

            expect(createdUrn).toBeNull();
            expect(message.error).toHaveBeenCalledWith('Failed to create document');
            // Should have called deleteNode twice: once for the temp node after success, once for rollback
            expect(mockDeleteNode).toHaveBeenCalled();
        });

        it('should handle creating document with parent and subType', async () => {
            const mockAddNode = vi.fn();
            const mockDeleteNode = vi.fn();

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: mockAddNode,
                deleteNode: mockDeleteNode,
                updateNodeTitle: vi.fn(),
                moveNode: vi.fn(),
                getNode: vi.fn(),
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const newUrn = 'urn:li:document:child123';
            const parentUrn = 'urn:li:document:parent456';
            const mocks = [
                {
                    request: {
                        query: CreateDocumentDocument,
                        variables: {
                            input: {
                                title: 'Child Document',
                                parentDocument: parentUrn,
                                subType: 'guide',
                                contents: { text: '' },
                            },
                        },
                    },
                    result: {
                        data: {
                            createDocument: newUrn,
                        },
                    },
                },
            ];

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useCreateDocumentTreeMutation(), { wrapper });

            const createdUrn = await result.current.createDocument({
                title: 'Child Document',
                parentDocument: parentUrn,
                subType: 'guide',
            });

            expect(createdUrn).toBe(newUrn);
            expect(mockAddNode).toHaveBeenCalledWith(
                expect.objectContaining({
                    urn: newUrn,
                    title: 'Child Document',
                    parentUrn,
                }),
            );
        });
    });

    describe('useUpdateDocumentTitleMutation', () => {
        it('should update title optimistically and persist', async () => {
            const mockUpdateNodeTitle = vi.fn();
            const mockGetNode = vi.fn(() => ({
                urn: 'urn:li:document:123',
                title: 'Old Title',
                parentUrn: null,
                hasChildren: false,
                children: [],
            }));

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: vi.fn(),
                deleteNode: vi.fn(),
                updateNodeTitle: mockUpdateNodeTitle,
                moveNode: vi.fn(),
                getNode: mockGetNode,
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentContentsDocument,
                        variables: {
                            input: {
                                urn: 'urn:li:document:123',
                                title: 'New Title',
                            },
                        },
                    },
                    result: {
                        data: {
                            updateDocumentContents: true,
                        },
                    },
                },
            ];

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useUpdateDocumentTitleMutation(), { wrapper });

            const success = await result.current.updateTitle('urn:li:document:123', 'New Title');

            expect(success).toBe(true);
            expect(mockUpdateNodeTitle).toHaveBeenCalledWith('urn:li:document:123', 'New Title');
        });

        it('should rollback title on error', async () => {
            const mockUpdateNodeTitle = vi.fn();
            const mockGetNode = vi.fn(() => ({
                urn: 'urn:li:document:123',
                title: 'Old Title',
                parentUrn: null,
                hasChildren: false,
                children: [],
            }));

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: vi.fn(),
                deleteNode: vi.fn(),
                updateNodeTitle: mockUpdateNodeTitle,
                moveNode: vi.fn(),
                getNode: mockGetNode,
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentContentsDocument,
                        variables: {
                            input: {
                                urn: 'urn:li:document:123',
                                title: 'New Title',
                            },
                        },
                    },
                    error: new Error('Server error'),
                },
            ];

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useUpdateDocumentTitleMutation(), { wrapper });

            const success = await result.current.updateTitle('urn:li:document:123', 'New Title');

            expect(success).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Failed to update title');
            // Should have called updateNodeTitle twice: once optimistically, once to rollback
            expect(mockUpdateNodeTitle).toHaveBeenCalledTimes(2);
            expect(mockUpdateNodeTitle).toHaveBeenLastCalledWith('urn:li:document:123', 'Old Title');
        });
    });

    describe('useMoveDocumentTreeMutation', () => {
        it('should move document optimistically and persist', async () => {
            const mockMoveNode = vi.fn();
            const mockGetNode = vi.fn(() => ({
                urn: 'urn:li:document:123',
                title: 'Document',
                parentUrn: 'urn:li:document:oldParent',
                hasChildren: false,
                children: [],
            }));

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: vi.fn(),
                deleteNode: vi.fn(),
                updateNodeTitle: vi.fn(),
                moveNode: mockMoveNode,
                getNode: mockGetNode,
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const mocks = [
                {
                    request: {
                        query: MoveDocumentDocument,
                        variables: {
                            input: {
                                urn: 'urn:li:document:123',
                                parentDocument: 'urn:li:document:newParent',
                            },
                        },
                    },
                    result: {
                        data: {
                            moveDocument: true,
                        },
                    },
                },
            ];

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useMoveDocumentTreeMutation(), { wrapper });

            const success = await result.current.moveDocument('urn:li:document:123', 'urn:li:document:newParent');

            expect(success).toBe(true);
            expect(mockMoveNode).toHaveBeenCalledWith('urn:li:document:123', 'urn:li:document:newParent');
            expect(message.success).toHaveBeenCalledWith('Document moved successfully');
        });

        it('should rollback move on error', async () => {
            const mockMoveNode = vi.fn();
            const mockGetNode = vi.fn(() => ({
                urn: 'urn:li:document:123',
                title: 'Document',
                parentUrn: 'urn:li:document:oldParent',
                hasChildren: false,
                children: [],
            }));

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: vi.fn(),
                deleteNode: vi.fn(),
                updateNodeTitle: vi.fn(),
                moveNode: mockMoveNode,
                getNode: mockGetNode,
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const mocks = [
                {
                    request: {
                        query: MoveDocumentDocument,
                        variables: {
                            input: {
                                urn: 'urn:li:document:123',
                                parentDocument: 'urn:li:document:newParent',
                            },
                        },
                    },
                    error: new Error('Server error'),
                },
            ];

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useMoveDocumentTreeMutation(), { wrapper });

            const success = await result.current.moveDocument('urn:li:document:123', 'urn:li:document:newParent');

            expect(success).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Failed to move document');
            // Should have called moveNode twice: once optimistically, once to rollback
            expect(mockMoveNode).toHaveBeenCalledTimes(2);
            expect(mockMoveNode).toHaveBeenLastCalledWith('urn:li:document:123', 'urn:li:document:oldParent');
        });

        it('should return false when document not found in tree', async () => {
            const mockGetNode = vi.fn(() => undefined);

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: vi.fn(),
                deleteNode: vi.fn(),
                updateNodeTitle: vi.fn(),
                moveNode: vi.fn(),
                getNode: mockGetNode,
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={[]} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useMoveDocumentTreeMutation(), { wrapper });

            const success = await result.current.moveDocument('urn:li:document:nonexistent', 'urn:li:document:parent');

            expect(success).toBe(false);
        });
    });

    describe('useDeleteDocumentTreeMutation', () => {
        it('should delete document optimistically and persist', async () => {
            const mockDeleteNode = vi.fn();
            const mockGetNode = vi.fn(() => ({
                urn: 'urn:li:document:123',
                title: 'Document to Delete',
                parentUrn: null,
                hasChildren: false,
                children: [],
            }));

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: vi.fn(),
                deleteNode: mockDeleteNode,
                updateNodeTitle: vi.fn(),
                moveNode: vi.fn(),
                getNode: mockGetNode,
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const mocks = [
                {
                    request: {
                        query: DeleteDocumentDocument,
                        variables: {
                            urn: 'urn:li:document:123',
                        },
                    },
                    result: {
                        data: {
                            deleteDocument: true,
                        },
                    },
                },
            ];

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useDeleteDocumentTreeMutation(), { wrapper });

            const success = await result.current.deleteDocument('urn:li:document:123');

            expect(success).toBe(true);
            expect(mockDeleteNode).toHaveBeenCalledWith('urn:li:document:123');
            expect(message.success).toHaveBeenCalledWith('Document deleted');
        });

        it('should rollback delete on error', async () => {
            const mockDeleteNode = vi.fn();
            const mockAddNode = vi.fn();
            const savedNode = {
                urn: 'urn:li:document:123',
                title: 'Document to Delete',
                parentUrn: null,
                hasChildren: false,
                children: [],
            };
            const mockGetNode = vi.fn(() => savedNode);

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: mockAddNode,
                deleteNode: mockDeleteNode,
                updateNodeTitle: vi.fn(),
                moveNode: vi.fn(),
                getNode: mockGetNode,
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const mocks = [
                {
                    request: {
                        query: DeleteDocumentDocument,
                        variables: {
                            urn: 'urn:li:document:123',
                        },
                    },
                    error: new Error('Server error'),
                },
            ];

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useDeleteDocumentTreeMutation(), { wrapper });

            const success = await result.current.deleteDocument('urn:li:document:123');

            expect(success).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Failed to delete document');
            expect(mockDeleteNode).toHaveBeenCalledWith('urn:li:document:123');
            expect(mockAddNode).toHaveBeenCalledWith(savedNode);
        });

        it('should return false when document not found in tree', async () => {
            const mockGetNode = vi.fn(() => undefined);

            const mockContextValue = {
                nodes: new Map(),
                rootUrns: [],
                addNode: vi.fn(),
                deleteNode: vi.fn(),
                updateNodeTitle: vi.fn(),
                moveNode: vi.fn(),
                getNode: mockGetNode,
                getRootNodes: vi.fn(),
                getChildren: vi.fn(),
                initializeTree: vi.fn(),
                setNodeChildren: vi.fn(),
            };

            const wrapper = ({ children }: any) => (
                <DocumentTreeContext.Provider value={mockContextValue}>
                    <MockedProvider mocks={[]} addTypename={false}>
                        {children}
                    </MockedProvider>
                </DocumentTreeContext.Provider>
            );

            const { result } = renderHook(() => useDeleteDocumentTreeMutation(), { wrapper });

            const success = await result.current.deleteDocument('urn:li:document:nonexistent');

            expect(success).toBe(false);
        });
    });
});
