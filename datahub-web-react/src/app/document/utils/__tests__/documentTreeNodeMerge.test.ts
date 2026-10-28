import { describe, expect, it } from 'vitest';

import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import {
    DEFAULT_DOCUMENT_TITLE,
    mergeServerChildNode,
    shouldFetchChildrenOnExpand,
} from '@app/document/utils/documentTreeNodeMerge';

function makeNode(overrides: Partial<DocumentTreeNode> = {}): DocumentTreeNode {
    return {
        urn: 'urn:li:document:test',
        title: 'Test',
        parentUrn: null,
        hasChildren: false,
        ...overrides,
    };
}

describe('mergeServerChildNode', () => {
    it('returns the server node when nothing exists locally', () => {
        const server = makeNode({ urn: 'a', title: 'From server' });
        expect(mergeServerChildNode(undefined, server)).toBe(server);
    });

    it('keeps an already-loaded children subtree from the existing node', () => {
        const grandchild = makeNode({ urn: 'g', title: 'Grandchild' });
        const existing = makeNode({
            urn: 'a',
            title: 'Stale title',
            hasChildren: true,
            children: [grandchild],
        });
        const server = makeNode({ urn: 'a', title: 'Fresh title', hasChildren: true });

        expect(mergeServerChildNode(existing, server)).toEqual({
            ...server,
            title: 'Fresh title',
            children: [grandchild],
            hasChildren: true,
        });
    });

    it('uses server children when the existing node was never loaded', () => {
        const existing = makeNode({ urn: 'a', hasChildren: true, children: undefined });
        const serverChild = makeNode({ urn: 'b' });
        const server = makeNode({ urn: 'a', hasChildren: true, children: [serverChild] });

        expect(mergeServerChildNode(existing, server).children).toEqual([serverChild]);
    });

    it('preserves hasChildren if either side reports children', () => {
        const existing = makeNode({ urn: 'a', hasChildren: true, children: [] });
        const server = makeNode({ urn: 'a', hasChildren: false });
        expect(mergeServerChildNode(existing, server).hasChildren).toBe(true);
    });

    it('keeps a local rename when search still returns the create-time default title', () => {
        const existing = makeNode({ urn: 'a', title: 'Renamed Doc' });
        const server = makeNode({ urn: 'a', title: DEFAULT_DOCUMENT_TITLE });
        expect(mergeServerChildNode(existing, server).title).toBe('Renamed Doc');
    });

    it('still takes a non-default server title over a local title', () => {
        const existing = makeNode({ urn: 'a', title: 'Local' });
        const server = makeNode({ urn: 'a', title: 'From search' });
        expect(mergeServerChildNode(existing, server).title).toBe('From search');
    });
});

describe('shouldFetchChildrenOnExpand', () => {
    it('fetches when the node has children that have never been loaded', () => {
        expect(shouldFetchChildrenOnExpand(makeNode({ hasChildren: true, children: undefined }))).toBe(true);
    });

    it('does not fetch leaves or already-loaded folders', () => {
        expect(shouldFetchChildrenOnExpand(makeNode({ hasChildren: false }))).toBe(false);
        expect(shouldFetchChildrenOnExpand(makeNode({ hasChildren: true, children: [] }))).toBe(false);
        expect(shouldFetchChildrenOnExpand(makeNode({ hasChildren: true, children: [makeNode({ urn: 'c' })] }))).toBe(
            false,
        );
    });
});
