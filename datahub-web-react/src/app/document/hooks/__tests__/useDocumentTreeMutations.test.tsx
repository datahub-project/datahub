import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import analytics, { DocumentEditType, EventType } from '@app/analytics';
import { DocumentTreeNode, DocumentTreeProvider, useDocumentTree } from '@app/document/DocumentTreeContext';
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
import { DocumentState } from '@types';

// Mock antd message
vi.mock('antd', () => ({
    message: {
        error: vi.fn(),
        success: vi.fn(),
    },
}));

// Mock analytics
vi.mock('@app/analytics', () => ({
    default: {
        event: vi.fn(),
    },
    EventType: {
        CreateDocumentEvent: 'CreateDocumentEvent',
        EditDocumentEvent: 'EditDocumentEvent',
        MoveDocumentEvent: 'MoveDocumentEvent',
        DeleteDocumentEvent: 'DeleteDocumentEvent',
    },
    DocumentEditType: {
        Title: 'Title',
    },
}));

const createWrapper = (mocks: any[]) => {
    return ({ children }: { children: React.ReactNode }) => (
        <MockedProvider mocks={mocks} addTypename={false}>
            <DocumentTreeProvider>{children}</DocumentTreeProvider>
        </MockedProvider>
    );
};

describe('useDocumentTreeMutations', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        console.error = vi.fn();
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    describe('useCreateDocumentTreeMutation', () => {
        it('should successfully create a root document', async () => {
            const mockUrn = 'urn:li:document:123';
            const input = {
                title: 'New Document',
                parentDocument: null,
                subType: 'guide',
            };

            const mocks = [
                {
                    request: {
                        query: CreateDocumentDocument,
                        variables: {
                            input: {
                                title: 'New Document',
                                parentDocument: null,
                                subType: 'guide',
                                state: DocumentState.Published,
                                contents: { text: '' },
                                settings: { showInGlobalContext: true },
                            },
                        },
                    },
                    result: {
                        data: {
                            createDocument: mockUrn,
                        },
                    },
                },
            ];

            const { result } = renderHook(
                () => ({
                    createMutation: useCreateDocumentTreeMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            const newUrn = await result.current.createMutation.createDocument(input);

            await waitFor(() => {
                expect(newUrn).toBe(mockUrn);
                // Verify analytics was called
                expect(analytics.event).toHaveBeenCalledWith({
                    type: EventType.CreateDocumentEvent,
                    documentUrn: mockUrn,
                    documentType: 'guide',
                    hasParent: false,
                    parentDocumentUrn: undefined,
                });
                // Verify node was added to tree
                const node = result.current.tree.getNode(mockUrn);
                expect(node).toBeTruthy();
                expect(node?.title).toBe('New Document');
            });
        });

        it('should successfully create a child document', async () => {
            const parentUrn = 'urn:li:document:parent';
            const mockUrn = 'urn:li:document:child';
            const input = {
                title: 'Child Document',
                parentDocument: parentUrn,
                subType: 'reference',
            };

            const mocks = [
                {
                    request: {
                        query: CreateDocumentDocument,
                        variables: {
                            input: {
                                title: 'Child Document',
                                parentDocument: parentUrn,
                                subType: 'reference',
                                state: DocumentState.Published,
                                contents: { text: '' },
                                settings: { showInGlobalContext: true },
                            },
                        },
                    },
                    result: {
                        data: {
                            createDocument: mockUrn,
                        },
                    },
                },
            ];

            const { result } = renderHook(() => useCreateDocumentTreeMutation(), {
                wrapper: createWrapper(mocks),
            });

            const newUrn = await result.current.createDocument(input);

            await waitFor(() => {
                expect(newUrn).toBe(mockUrn);
                expect(analytics.event).toHaveBeenCalledWith({
                    type: EventType.CreateDocumentEvent,
                    documentUrn: mockUrn,
                    documentType: 'reference',
                    hasParent: true,
                    parentDocumentUrn: parentUrn,
                });
            });
        });

        it('should rollback on mutation error', async () => {
            const input = {
                title: 'Failed Document',
                parentDocument: null,
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
                                state: DocumentState.Published,
                                contents: { text: '' },
                                settings: { showInGlobalContext: true },
                            },
                        },
                    },
                    error: new Error('Network error'),
                },
            ];

            const { result } = renderHook(
                () => ({
                    createMutation: useCreateDocumentTreeMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            const newUrn = await result.current.createMutation.createDocument(input);

            await waitFor(() => {
                expect(newUrn).toBe(null);
                expect(message.error).toHaveBeenCalledWith('Failed to create document');
                // Verify no nodes remain in tree (temp node was rolled back)
                const rootNodes = result.current.tree.getRootNodes();
                expect(rootNodes).toHaveLength(0);
            });
        });

        it('should rollback when mutation returns no URN', async () => {
            const input = {
                title: 'Failed Document',
                parentDocument: null,
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
                                state: DocumentState.Published,
                                contents: { text: '' },
                                settings: { showInGlobalContext: true },
                            },
                        },
                    },
                    result: {
                        data: {
                            createDocument: null,
                        },
                    },
                },
            ];

            const { result } = renderHook(
                () => ({
                    createMutation: useCreateDocumentTreeMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            const newUrn = await result.current.createMutation.createDocument(input);

            await waitFor(() => {
                expect(newUrn).toBe(null);
                expect(message.error).toHaveBeenCalledWith('Failed to create document');
            });
        });
    });

    describe('useUpdateDocumentTitleMutation', () => {
        it('should successfully update document title', async () => {
            const mockUrn = 'urn:li:document:123';
            const newTitle = 'Updated Title';

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentContentsDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                title: newTitle,
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

            const { result } = renderHook(
                () => ({
                    updateMutation: useUpdateDocumentTitleMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            // Add node to tree first
            const initialNode: DocumentTreeNode = {
                urn: mockUrn,
                title: 'Old Title',
                parentUrn: null,
                hasChildren: false,
                children: [],
            };
            result.current.tree.addNode(initialNode);

            const success = await result.current.updateMutation.updateTitle(mockUrn, newTitle);

            await waitFor(() => {
                expect(success).toBe(true);
                expect(analytics.event).toHaveBeenCalledWith({
                    type: EventType.EditDocumentEvent,
                    documentUrn: mockUrn,
                    editType: DocumentEditType.Title,
                });
                // Verify title was updated in tree
                const node = result.current.tree.getNode(mockUrn);
                expect(node?.title).toBe(newTitle);
            });
        });

        it('should rollback on mutation error', async () => {
            const mockUrn = 'urn:li:document:123';
            const oldTitle = 'Old Title';
            const newTitle = 'Failed Title';

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentContentsDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                title: newTitle,
                            },
                        },
                    },
                    error: new Error('Update failed'),
                },
            ];

            const { result } = renderHook(
                () => ({
                    updateMutation: useUpdateDocumentTitleMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            // Add node to tree first
            const initialNode: DocumentTreeNode = {
                urn: mockUrn,
                title: oldTitle,
                parentUrn: null,
                hasChildren: false,
                children: [],
            };
            result.current.tree.addNode(initialNode);

            const success = await result.current.updateMutation.updateTitle(mockUrn, newTitle);

            await waitFor(() => {
                expect(success).toBe(false);
                expect(message.error).toHaveBeenCalledWith('Failed to update title');
                // Verify title was rolled back
                const node = result.current.tree.getNode(mockUrn);
                expect(node?.title).toBe(oldTitle);
            });
        });

        it('should handle updating non-existent document gracefully', async () => {
            const mockUrn = 'urn:li:document:nonexistent';
            const newTitle = 'New Title';

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentContentsDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                title: newTitle,
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

            const { result } = renderHook(() => useUpdateDocumentTitleMutation(), {
                wrapper: createWrapper(mocks),
            });

            const success = await result.current.updateTitle(mockUrn, newTitle);

            await waitFor(() => {
                expect(success).toBe(true);
            });
        });
    });

    describe('useMoveDocumentTreeMutation', () => {
        it('should successfully move document to new parent', async () => {
            const mockUrn = 'urn:li:document:child';
            const oldParentUrn = 'urn:li:document:oldParent';
            const newParentUrn = 'urn:li:document:newParent';

            const mocks = [
                {
                    request: {
                        query: MoveDocumentDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                parentDocument: newParentUrn,
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

            const { result } = renderHook(
                () => ({
                    moveMutation: useMoveDocumentTreeMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            // Add node to tree first
            const initialNode: DocumentTreeNode = {
                urn: mockUrn,
                title: 'Child Document',
                parentUrn: oldParentUrn,
                hasChildren: false,
                children: [],
            };
            result.current.tree.addNode(initialNode);

            const success = await result.current.moveMutation.moveDocument(mockUrn, newParentUrn);

            await waitFor(() => {
                expect(success).toBe(true);
                expect(message.success).toHaveBeenCalledWith('Document moved successfully');
                expect(analytics.event).toHaveBeenCalledWith({
                    type: EventType.MoveDocumentEvent,
                    documentUrn: mockUrn,
                    oldParentDocumentUrn: oldParentUrn,
                    newParentDocumentUrn: newParentUrn,
                });
                // Verify parent was updated in tree
                const node = result.current.tree.getNode(mockUrn);
                expect(node?.parentUrn).toBe(newParentUrn);
            });
        });

        it('should successfully move document to root level', async () => {
            const mockUrn = 'urn:li:document:child';
            const oldParentUrn = 'urn:li:document:parent';

            const mocks = [
                {
                    request: {
                        query: MoveDocumentDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                parentDocument: null,
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

            const { result } = renderHook(
                () => ({
                    moveMutation: useMoveDocumentTreeMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            const initialNode: DocumentTreeNode = {
                urn: mockUrn,
                title: 'Child Document',
                parentUrn: oldParentUrn,
                hasChildren: false,
                children: [],
            };
            result.current.tree.addNode(initialNode);

            const success = await result.current.moveMutation.moveDocument(mockUrn, null);

            await waitFor(() => {
                expect(success).toBe(true);
                const node = result.current.tree.getNode(mockUrn);
                expect(node?.parentUrn).toBe(null);
            });
        });

        it('should rollback on mutation error', async () => {
            const mockUrn = 'urn:li:document:child';
            const oldParentUrn = 'urn:li:document:oldParent';
            const newParentUrn = 'urn:li:document:newParent';

            const mocks = [
                {
                    request: {
                        query: MoveDocumentDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                parentDocument: newParentUrn,
                            },
                        },
                    },
                    error: new Error('Move failed'),
                },
            ];

            const { result } = renderHook(
                () => ({
                    moveMutation: useMoveDocumentTreeMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            const initialNode: DocumentTreeNode = {
                urn: mockUrn,
                title: 'Child Document',
                parentUrn: oldParentUrn,
                hasChildren: false,
                children: [],
            };
            result.current.tree.addNode(initialNode);

            const success = await result.current.moveMutation.moveDocument(mockUrn, newParentUrn);

            await waitFor(() => {
                expect(success).toBe(false);
                expect(message.error).toHaveBeenCalledWith('Failed to move document');
                // Verify parent was rolled back
                const node = result.current.tree.getNode(mockUrn);
                expect(node?.parentUrn).toBe(oldParentUrn);
            });
        });

        it('should return false when document not found in tree', async () => {
            const mockUrn = 'urn:li:document:nonexistent';

            const mocks: any[] = [];

            const { result } = renderHook(() => useMoveDocumentTreeMutation(), {
                wrapper: createWrapper(mocks),
            });

            const success = await result.current.moveDocument(mockUrn, null);

            expect(success).toBe(false);
        });
    });

    describe('useDeleteDocumentTreeMutation', () => {
        it('should successfully delete document that exists in tree', async () => {
            const mockUrn = 'urn:li:document:123';

            const mocks = [
                {
                    request: {
                        query: DeleteDocumentDocument,
                        variables: { urn: mockUrn },
                    },
                    result: {
                        data: {
                            deleteDocument: true,
                        },
                    },
                },
            ];

            const { result } = renderHook(
                () => ({
                    deleteMutation: useDeleteDocumentTreeMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            // Add node to tree first
            const initialNode: DocumentTreeNode = {
                urn: mockUrn,
                title: 'Document to Delete',
                parentUrn: null,
                hasChildren: false,
                children: [],
            };
            result.current.tree.addNode(initialNode);

            const success = await result.current.deleteMutation.deleteDocument(mockUrn);

            await waitFor(() => {
                expect(success).toBe(true);
                expect(message.success).toHaveBeenCalledWith('Document deleted');
                expect(analytics.event).toHaveBeenCalledWith({
                    type: EventType.DeleteDocumentEvent,
                    documentUrn: mockUrn,
                });
                // Verify node was removed from tree
                const node = result.current.tree.getNode(mockUrn);
                expect(node).toBe(undefined);
            });
        });

        it('should successfully delete document not in tree (e.g., modal)', async () => {
            const mockUrn = 'urn:li:document:notInTree';

            const mocks = [
                {
                    request: {
                        query: DeleteDocumentDocument,
                        variables: { urn: mockUrn },
                    },
                    result: {
                        data: {
                            deleteDocument: true,
                        },
                    },
                },
            ];

            const { result } = renderHook(() => useDeleteDocumentTreeMutation(), {
                wrapper: createWrapper(mocks),
            });

            const success = await result.current.deleteDocument(mockUrn);

            await waitFor(() => {
                expect(success).toBe(true);
                expect(message.success).toHaveBeenCalledWith('Document deleted');
            });
        });

        it('should rollback on mutation error', async () => {
            const mockUrn = 'urn:li:document:123';

            const mocks = [
                {
                    request: {
                        query: DeleteDocumentDocument,
                        variables: { urn: mockUrn },
                    },
                    error: new Error('Delete failed'),
                },
            ];

            const { result } = renderHook(
                () => ({
                    deleteMutation: useDeleteDocumentTreeMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            const initialNode: DocumentTreeNode = {
                urn: mockUrn,
                title: 'Document to Delete',
                parentUrn: null,
                hasChildren: false,
                children: [],
            };
            result.current.tree.addNode(initialNode);

            const success = await result.current.deleteMutation.deleteDocument(mockUrn);

            await waitFor(() => {
                expect(success).toBe(false);
                expect(message.error).toHaveBeenCalledWith('Failed to delete document');
                // Verify node was restored in tree
                const node = result.current.tree.getNode(mockUrn);
                expect(node).toBeTruthy();
                expect(node?.title).toBe('Document to Delete');
            });
        });

        it('should rollback when mutation returns false', async () => {
            const mockUrn = 'urn:li:document:123';

            const mocks = [
                {
                    request: {
                        query: DeleteDocumentDocument,
                        variables: { urn: mockUrn },
                    },
                    result: {
                        data: {
                            deleteDocument: false,
                        },
                    },
                },
            ];

            const { result } = renderHook(
                () => ({
                    deleteMutation: useDeleteDocumentTreeMutation(),
                    tree: useDocumentTree(),
                }),
                {
                    wrapper: createWrapper(mocks),
                },
            );

            const initialNode: DocumentTreeNode = {
                urn: mockUrn,
                title: 'Document to Delete',
                parentUrn: null,
                hasChildren: false,
                children: [],
            };
            result.current.tree.addNode(initialNode);

            const success = await result.current.deleteMutation.deleteDocument(mockUrn);

            await waitFor(() => {
                expect(success).toBe(false);
                expect(message.error).toHaveBeenCalledWith('Failed to delete document');
                // Verify node was restored
                const node = result.current.tree.getNode(mockUrn);
                expect(node).toBeTruthy();
            });
        });
    });
});
