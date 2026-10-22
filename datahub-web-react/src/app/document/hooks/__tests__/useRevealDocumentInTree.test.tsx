import { waitFor } from '@testing-library/react';
import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DocumentTreeNode, DocumentTreeProvider, useDocumentTree } from '@app/document/DocumentTreeContext';
import { useRevealDocumentInTree } from '@app/document/hooks/useRevealDocumentInTree';

const mockGetCurrentDocumentUrn = vi.fn();
const mockUseGetDocumentQuery = vi.fn();

vi.mock('@app/document/hooks/useDocumentNavigation', () => ({
    useDocumentNavigation: () => ({
        getCurrentDocumentUrn: mockGetCurrentDocumentUrn,
    }),
}));

vi.mock('@graphql/document.generated', () => ({
    useGetDocumentQuery: (opts: unknown) => mockUseGetDocumentQuery(opts),
}));

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <DocumentTreeProvider>{children}</DocumentTreeProvider>
);

function makeNode(overrides: Partial<DocumentTreeNode> = {}): DocumentTreeNode {
    return { urn: 'urn:li:document:test', title: 'Test', parentUrn: null, hasChildren: false, ...overrides };
}

describe('useRevealDocumentInTree', () => {
    const loadChildren = vi.fn();
    const loadMoreChildren = vi.fn();
    const hasMoreChildren = vi.fn(() => false);

    beforeEach(() => {
        vi.clearAllMocks();
        mockGetCurrentDocumentUrn.mockReturnValue('urn:li:document:child');
        loadChildren.mockResolvedValue([]);
        loadMoreChildren.mockResolvedValue([]);
        hasMoreChildren.mockReturnValue(false);
    });

    it('retries nested reveal after a late source-platform root is appended', async () => {
        mockUseGetDocumentQuery.mockReturnValue({
            data: {
                document: {
                    urn: 'urn:li:document:child',
                    info: { title: 'Child', lastModified: { time: 1 } },
                    platform: { urn: 'urn:li:dataPlatform:confluence', name: 'confluence' },
                    parentDocuments: {
                        documents: [{ urn: 'urn:li:document:confluence-root', info: { title: 'Confluence Root' } }],
                    },
                },
            },
            loading: false,
        });

        const { result } = renderHook(
            () => {
                const tree = useDocumentTree();
                useRevealDocumentInTree({
                    loadChildren,
                    loadMoreChildren,
                    hasMoreChildren,
                    rootsLoading: false,
                });
                return tree;
            },
            { wrapper },
        );

        // Spinner already cleared; page 0 had no Confluence roots yet.
        expect(loadChildren).not.toHaveBeenCalled();

        await act(async () => {
            result.current.appendRootNodes([
                makeNode({
                    urn: 'urn:li:document:confluence-root',
                    title: 'Confluence Root',
                    hasChildren: true,
                }),
            ]);
        });

        await waitFor(() => {
            expect(loadChildren).toHaveBeenCalledWith('urn:li:document:confluence-root');
        });
        expect(result.current.expandedUrns.has('urn:li:document:confluence-root')).toBe(true);
    });

    it('ignores a stale nodes-map root until it is back in rootUrns', async () => {
        mockUseGetDocumentQuery.mockReturnValue({
            data: {
                document: {
                    urn: 'urn:li:document:child',
                    info: { title: 'Child', lastModified: { time: 1 } },
                    parentDocuments: {
                        documents: [{ urn: 'urn:li:document:stale-root', info: { title: 'Stale Root' } }],
                    },
                },
            },
            loading: false,
        });

        const { result } = renderHook(
            () => {
                const tree = useDocumentTree();
                useRevealDocumentInTree({
                    loadChildren,
                    loadMoreChildren,
                    hasMoreChildren,
                    rootsLoading: false,
                });
                return tree;
            },
            { wrapper },
        );

        // Scroll a root in, then replace page 0 — mergeInitializedRootUrns drops it from
        // rootUrns while the nodes map still holds the stale entry.
        await act(async () => {
            result.current.initializeTree([
                makeNode({ urn: 'urn:li:document:page0', title: 'Page0', hasChildren: false }),
            ]);
            result.current.appendRootNodes([
                makeNode({
                    urn: 'urn:li:document:stale-root',
                    title: 'Stale Root',
                    hasChildren: true,
                }),
            ]);
        });
        await act(async () => {
            result.current.initializeTree([
                makeNode({ urn: 'urn:li:document:other-root', title: 'Other', hasChildren: false }),
            ]);
        });

        expect(result.current.rootUrns).not.toContain('urn:li:document:stale-root');
        expect(result.current.getNode('urn:li:document:stale-root')).toBeDefined();
        // Reveal may have run while the root was briefly in rootUrns; a stale nodes
        // entry alone must not keep triggering it after the page-0 reset.
        loadChildren.mockClear();
        await act(async () => {
            await Promise.resolve();
        });
        expect(loadChildren).not.toHaveBeenCalled();

        await act(async () => {
            result.current.appendRootNodes([
                makeNode({
                    urn: 'urn:li:document:stale-root',
                    title: 'Stale Root',
                    hasChildren: true,
                }),
            ]);
        });

        await waitFor(() => {
            expect(loadChildren).toHaveBeenCalledWith('urn:li:document:stale-root');
        });
    });

    it('does not reveal while roots are still initializing', async () => {
        mockUseGetDocumentQuery.mockReturnValue({
            data: undefined,
            loading: false,
        });

        renderHook(
            () => {
                useRevealDocumentInTree({
                    loadChildren,
                    loadMoreChildren,
                    hasMoreChildren,
                    rootsLoading: true,
                });
            },
            { wrapper },
        );

        expect(mockUseGetDocumentQuery).toHaveBeenCalledWith(
            expect.objectContaining({
                skip: true,
            }),
        );
        expect(loadChildren).not.toHaveBeenCalled();
    });
});
