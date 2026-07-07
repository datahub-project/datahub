import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DocumentTreeNode, DocumentTreeProvider, useDocumentTree } from '@app/document/DocumentTreeContext';
import { useNodeChildrenLoading } from '@app/document/hooks/useNodeChildrenLoading';

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <DocumentTreeProvider>{children}</DocumentTreeProvider>
);

function makeNode(overrides: Partial<DocumentTreeNode> = {}): DocumentTreeNode {
    return { urn: 'urn:li:document:test', title: 'Test', parentUrn: null, hasChildren: false, ...overrides };
}

describe('useNodeChildrenLoading', () => {
    let loadChildren: ReturnType<typeof vi.fn>;
    let loadMoreChildren: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        loadChildren = vi.fn().mockResolvedValue([]);
        loadMoreChildren = vi.fn().mockResolvedValue(undefined);
    });

    const render = () =>
        renderHook(
            () => ({
                loading: useNodeChildrenLoading({ loadChildren, loadMoreChildren }),
                tree: useDocumentTree(),
            }),
            { wrapper },
        );

    it('expands a collapsed folder and fetches its children', async () => {
        const { result } = render();
        act(() => {
            result.current.tree.addNode(makeNode({ urn: 'f', hasChildren: true }));
        });

        await act(async () => {
            await result.current.loading.handleToggleExpand('f');
        });

        expect(result.current.tree.expandedUrns.has('f')).toBe(true);
        expect(loadChildren).toHaveBeenCalledWith('f');
        // The in-flight marker is cleared once the fetch settles.
        expect(result.current.loading.loadingUrns.has('f')).toBe(false);
    });

    it('collapses an already-expanded folder without refetching', async () => {
        const { result } = render();
        act(() => {
            result.current.tree.addNode(makeNode({ urn: 'f', hasChildren: true }));
            result.current.tree.expandNode('f');
        });

        await act(async () => {
            await result.current.loading.handleToggleExpand('f');
        });

        expect(result.current.tree.expandedUrns.has('f')).toBe(false);
        expect(loadChildren).not.toHaveBeenCalled();
    });

    it('does nothing for an unknown urn', async () => {
        const { result } = render();
        await act(async () => {
            await result.current.loading.handleToggleExpand('missing');
        });

        expect(loadChildren).not.toHaveBeenCalled();
        expect(result.current.tree.expandedUrns.size).toBe(0);
    });

    it('loads the next page of children via handleLoadMoreChildren', async () => {
        const { result } = render();
        await act(async () => {
            await result.current.loading.handleLoadMoreChildren('p');
        });

        expect(loadMoreChildren).toHaveBeenCalledWith('p');
        expect(result.current.loading.loadingChildrenUrns.has('p')).toBe(false);
    });
});
