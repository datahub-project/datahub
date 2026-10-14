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
});
