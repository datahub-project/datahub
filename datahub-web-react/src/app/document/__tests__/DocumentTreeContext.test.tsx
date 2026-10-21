import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';

import { DocumentTreeNode, DocumentTreeProvider, useDocumentTree } from '@app/document/DocumentTreeContext';

function makeNode(urn: string, parentUrn: string | null = null): DocumentTreeNode {
    return {
        urn,
        title: urn,
        parentUrn,
        hasChildren: false,
    };
}

describe('DocumentTreeContext root replacement', () => {
    const wrapper = ({ children }: { children: React.ReactNode }) => (
        <DocumentTreeProvider>{children}</DocumentTreeProvider>
    );

    it('drops scrolled-in roots on the next first page and keeps an optimistic create', () => {
        const { result } = renderHook(() => useDocumentTree(), { wrapper });

        act(() => {
            result.current.initializeTree([makeNode('a'), makeNode('b')]);
            result.current.appendRootNodes([makeNode('c')]);
        });
        act(() => {
            result.current.addNode(makeNode('optimistic'), { placement: 'start' });
        });
        act(() => {
            result.current.initializeTree([makeNode('b'), makeNode('a')]);
        });

        expect(result.current.getRootNodes().map((node) => node.urn)).toEqual(['optimistic', 'b', 'a']);
    });

    it('keeps a document moved to root across a first-page reload', () => {
        const { result } = renderHook(() => useDocumentTree(), { wrapper });

        act(() => {
            result.current.addNode(makeNode('parent'));
            result.current.addNode(makeNode('child', 'parent'));
        });
        act(() => {
            result.current.moveNode('child', null);
        });
        act(() => {
            result.current.initializeTree([makeNode('parent')]);
        });

        expect(result.current.getRootNodes().map((node) => node.urn)).toEqual(['child', 'parent']);
    });

    it('keeps a local root pin when a platform prefetch appends that urn', () => {
        const { result } = renderHook(() => useDocumentTree(), { wrapper });

        act(() => {
            result.current.addNode(makeNode('created'), { placement: 'start' });
            result.current.appendRootNodes([makeNode('created'), makeNode('other-platform-root')], {
                preserveLocalPlacement: true,
            });
        });
        act(() => {
            result.current.initializeTree([makeNode('page0')]);
        });

        expect(result.current.getRootNodes().map((node) => node.urn)).toEqual(['created', 'page0']);
    });

    it('drops a local root pin once a global scroll page includes it', () => {
        const { result } = renderHook(() => useDocumentTree(), { wrapper });

        act(() => {
            result.current.initializeTree([makeNode('page0')]);
            result.current.addNode(makeNode('created'), { placement: 'start' });
        });
        act(() => {
            result.current.appendRootNodes([makeNode('created')]);
        });
        act(() => {
            result.current.initializeTree([makeNode('page0')]);
        });

        expect(result.current.getRootNodes().map((node) => node.urn)).toEqual(['page0']);
    });

    it('keeps loaded children when a later page appends the same root', () => {
        const { result } = renderHook(() => useDocumentTree(), { wrapper });

        act(() => {
            result.current.initializeTree([makeNode('root')]);
            result.current.setNodeChildren('root', [makeNode('child', 'root')]);
        });
        act(() => {
            result.current.appendRootNodes([{ ...makeNode('root'), hasChildren: true, children: undefined }]);
        });

        expect(result.current.getNode('root')?.children?.map((child) => child.urn)).toEqual(['child']);
    });

    it('drops a created root when a View reload clears the tree', () => {
        const { result } = renderHook(() => useDocumentTree(), { wrapper });

        act(() => {
            result.current.initializeTree([makeNode('server')]);
            result.current.addNode(makeNode('created'), { placement: 'start' });
            result.current.setNodeChildren('created', [makeNode('created-child', 'created')]);
        });
        act(() => {
            result.current.initializeTree([]);
        });

        expect(result.current.getRootNodes()).toEqual([]);
        expect(result.current.getNode('created')).toBeUndefined();

        act(() => {
            result.current.initializeTree([makeNode('next-page')]);
        });

        expect(result.current.getRootNodes().map((node) => node.urn)).toEqual(['next-page']);
    });
});
