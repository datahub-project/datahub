import { MockedProvider } from '@apollo/client/testing';
import { waitFor } from '@testing-library/react';
import { renderHook } from '@testing-library/react-hooks';
import { message } from 'antd';
import React from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
    UpdateDocumentContentsInput,
    UpdateDocumentRelatedEntitiesInput,
    UpdateDocumentStatusInput,
    UpdateDocumentSubTypeInput,
    useUpdateDocument,
} from '@app/document/hooks/useUpdateDocument';

import {
    UpdateDocumentContentsDocument,
    UpdateDocumentRelatedEntitiesDocument,
    UpdateDocumentStatusDocument,
    UpdateDocumentSubTypeDocument,
} from '@graphql/document.generated';
import { DocumentState } from '@types';

// Mock antd message
vi.mock('antd', () => ({
    message: {
        error: vi.fn(),
    },
}));

describe('useUpdateDocument', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        console.error = vi.fn(); // Suppress console.error in tests
    });

    afterEach(() => {
        vi.restoreAllMocks();
    });

    describe('updateContents', () => {
        it('should successfully update document contents with all fields', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentContentsInput = {
                urn: mockUrn,
                title: 'New Title',
                contents: { text: 'New content' },
                subType: 'guide',
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentContentsDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                title: 'New Title',
                                subType: 'guide',
                                contents: { text: 'New content' },
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

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateContents(input);

            expect(success).toBe(true);
            expect(message.error).not.toHaveBeenCalled();
        });

        it('should successfully update document contents without contents field', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentContentsInput = {
                urn: mockUrn,
                title: 'New Title',
                subType: 'guide',
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentContentsDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                title: 'New Title',
                                subType: 'guide',
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

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateContents(input);

            expect(success).toBe(true);
        });

        it('should handle failed update when mutation returns false', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentContentsInput = {
                urn: mockUrn,
                title: 'New Title',
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentContentsDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                title: 'New Title',
                                subType: undefined,
                            },
                        },
                    },
                    result: {
                        data: {
                            updateDocumentContents: false,
                        },
                    },
                },
            ];

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateContents(input);

            expect(success).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Failed to update document. An unexpected error occurred!');
        });

        it('should handle GraphQL errors', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentContentsInput = {
                urn: mockUrn,
                title: 'New Title',
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentContentsDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                title: 'New Title',
                                subType: undefined,
                            },
                        },
                    },
                    error: new Error('Network error'),
                },
            ];

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateContents(input);

            expect(success).toBe(false);
            expect(message.error).toHaveBeenCalledWith('Failed to update document. An unexpected error occurred!');
        });
    });

    describe('updateStatus', () => {
        it('should successfully update document status', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentStatusInput = {
                urn: mockUrn,
                state: DocumentState.Published,
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentStatusDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                state: DocumentState.Published,
                            },
                        },
                    },
                    result: {
                        data: {
                            updateDocumentStatus: true,
                        },
                    },
                },
            ];

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateStatus(input);

            expect(success).toBe(true);
            expect(message.error).not.toHaveBeenCalled();
        });

        it('should handle failed status update', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentStatusInput = {
                urn: mockUrn,
                state: DocumentState.Unpublished,
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentStatusDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                state: DocumentState.Unpublished,
                            },
                        },
                    },
                    result: {
                        data: {
                            updateDocumentStatus: false,
                        },
                    },
                },
            ];

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateStatus(input);

            expect(success).toBe(false);
            expect(message.error).toHaveBeenCalledWith(
                'Failed to update document status. An unexpected error occurred!',
            );
        });
    });

    describe('updateSubType', () => {
        it('should successfully update document subType', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentSubTypeInput = {
                urn: mockUrn,
                subType: 'tutorial',
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentSubTypeDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                subType: 'tutorial',
                            },
                        },
                    },
                    result: {
                        data: {
                            updateDocumentSubType: true,
                        },
                    },
                },
            ];

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateSubType(input);

            expect(success).toBe(true);
        });

        it('should handle null subType', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentSubTypeInput = {
                urn: mockUrn,
                subType: null,
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentSubTypeDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                subType: null,
                            },
                        },
                    },
                    result: {
                        data: {
                            updateDocumentSubType: true,
                        },
                    },
                },
            ];

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateSubType(input);

            expect(success).toBe(true);
        });

        it('should handle failed subType update', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentSubTypeInput = {
                urn: mockUrn,
                subType: 'guide',
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentSubTypeDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                subType: 'guide',
                            },
                        },
                    },
                    error: new Error('Server error'),
                },
            ];

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateSubType(input);

            expect(success).toBe(false);
            expect(message.error).toHaveBeenCalledWith(
                'Failed to update document sub-type. An unexpected error occurred!',
            );
        });
    });

    describe('updateRelatedEntities', () => {
        it('should successfully update related entities', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentRelatedEntitiesInput = {
                urn: mockUrn,
                relatedAssets: ['urn:li:dataset:1', 'urn:li:dataset:2'],
                relatedDocuments: ['urn:li:document:456'],
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentRelatedEntitiesDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                relatedAssets: ['urn:li:dataset:1', 'urn:li:dataset:2'],
                                relatedDocuments: ['urn:li:document:456'],
                            },
                        },
                    },
                    result: {
                        data: {
                            updateDocumentRelatedEntities: true,
                        },
                    },
                },
            ];

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateRelatedEntities(input);

            expect(success).toBe(true);
        });

        it('should handle failed related entities update', async () => {
            const mockUrn = 'urn:li:document:123';
            const input: UpdateDocumentRelatedEntitiesInput = {
                urn: mockUrn,
                relatedAssets: ['urn:li:dataset:1'],
            };

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentRelatedEntitiesDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                relatedAssets: ['urn:li:dataset:1'],
                                relatedDocuments: undefined,
                            },
                        },
                    },
                    result: {
                        data: {
                            updateDocumentRelatedEntities: false,
                        },
                    },
                },
            ];

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            const success = await result.current.updateRelatedEntities(input);

            expect(success).toBe(false);
            expect(message.error).toHaveBeenCalledWith(
                'Failed to update related assets. An unexpected error occurred!',
            );
        });
    });

    describe('loading states', () => {
        it('should track loading state correctly', async () => {
            const mockUrn = 'urn:li:document:123';

            const mocks = [
                {
                    request: {
                        query: UpdateDocumentContentsDocument,
                        variables: {
                            input: {
                                urn: mockUrn,
                                title: 'Test',
                                subType: undefined,
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

            const { result } = renderHook(() => useUpdateDocument(), {
                wrapper: ({ children }) => (
                    <MockedProvider mocks={mocks} addTypename={false}>
                        {children}
                    </MockedProvider>
                ),
            });

            expect(result.current.loading).toBe(false);

            const promise = result.current.updateContents({ urn: mockUrn, title: 'Test' });

            await waitFor(() => {
                expect(result.current.loading).toBe(false);
            });

            await promise;
        });
    });
});
