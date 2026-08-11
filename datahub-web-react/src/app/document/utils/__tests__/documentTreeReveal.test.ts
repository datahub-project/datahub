import { describe, expect, it, vi } from 'vitest';

import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import { revealDocumentInTree } from '@app/document/utils/documentTreeReveal';

function makeNode(overrides: Partial<DocumentTreeNode> = {}): DocumentTreeNode {
    return { urn: 'urn:li:document:test', title: 'Test', parentUrn: null, hasChildren: false, ...overrides };
}

describe('revealDocumentInTree', () => {
    it('no-ops for a root document missing from the loaded window (keeps sort + scroll at top)', async () => {
        const ensureNode = vi.fn();
        const expandNode = vi.fn();
        const loadChildren = vi.fn().mockResolvedValue([]);

        await revealDocumentInTree({
            documentUrn: 'urn:li:document:root',
            documentTitle: 'Root Doc',
            parentDocuments: [],
            getNode: () => undefined,
            expandNode,
            ensureNode,
            loadChildren,
            loadMoreChildren: vi.fn().mockResolvedValue([]),
            hasMoreChildren: () => false,
        });

        expect(ensureNode).not.toHaveBeenCalled();
        expect(expandNode).not.toHaveBeenCalled();
        expect(loadChildren).not.toHaveBeenCalled();
    });

    it('no-ops when nested doc root ancestor is not in the loaded window', async () => {
        const ensureNode = vi.fn();
        const loadChildren = vi.fn().mockResolvedValue([]);

        await revealDocumentInTree({
            documentUrn: 'urn:li:document:c',
            documentTitle: 'C',
            parentDocuments: [
                { urn: 'urn:li:document:b', title: 'B' },
                { urn: 'urn:li:document:a', title: 'A' },
            ],
            getNode: () => undefined,
            expandNode: vi.fn(),
            ensureNode,
            loadChildren,
            loadMoreChildren: vi.fn().mockResolvedValue([]),
            hasMoreChildren: () => false,
        });

        expect(ensureNode).not.toHaveBeenCalled();
        expect(loadChildren).not.toHaveBeenCalled();
    });

    it('expands ancestors root-first and loads children along the path when root is loaded', async () => {
        const nodes = new Map<string, DocumentTreeNode>([
            ['urn:li:document:a', makeNode({ urn: 'urn:li:document:a', title: 'A', hasChildren: true })],
        ]);
        const expanded = new Set<string>();
        const childPages = new Map<string, DocumentTreeNode[][]>([
            [
                'urn:li:document:a',
                [[makeNode({ urn: 'urn:li:document:b', title: 'B', parentUrn: 'urn:li:document:a' })]],
            ],
            [
                'urn:li:document:b',
                [[makeNode({ urn: 'urn:li:document:c', title: 'C', parentUrn: 'urn:li:document:b' })]],
            ],
        ]);

        const ensureNode = vi.fn((node: DocumentTreeNode) => {
            nodes.set(node.urn, node);
        });
        const expandNode = vi.fn((urn: string) => {
            expanded.add(urn);
        });
        const loadChildren = vi.fn(async (parentUrn: string) => {
            const pages = childPages.get(parentUrn) || [[]];
            const first = pages[0] || [];
            first.forEach((n) => nodes.set(n.urn, n));
            return first;
        });
        const loadMoreChildren = vi.fn().mockResolvedValue([]);

        await revealDocumentInTree({
            documentUrn: 'urn:li:document:c',
            documentTitle: 'C',
            // API order: direct parent first
            parentDocuments: [
                { urn: 'urn:li:document:b', title: 'B' },
                { urn: 'urn:li:document:a', title: 'A' },
            ],
            getNode: (urn) => nodes.get(urn),
            expandNode,
            ensureNode,
            loadChildren,
            loadMoreChildren,
            hasMoreChildren: () => false,
        });

        expect(expandNode.mock.calls.map((c) => c[0])).toEqual(['urn:li:document:a', 'urn:li:document:b']);
        expect(loadChildren.mock.calls.map((c) => c[0])).toEqual(['urn:li:document:a', 'urn:li:document:b']);
        expect(nodes.get('urn:li:document:c')?.parentUrn).toBe('urn:li:document:b');
    });

    it('pages through children until the next path node appears', async () => {
        const nodes = new Map<string, DocumentTreeNode>([
            ['urn:li:document:a', makeNode({ urn: 'urn:li:document:a', title: 'A', hasChildren: true })],
        ]);
        const ensureNode = vi.fn((node: DocumentTreeNode) => {
            nodes.set(node.urn, node);
        });
        const expandNode = vi.fn();
        const page1 = [makeNode({ urn: 'urn:li:document:other', parentUrn: 'urn:li:document:a' })];
        const page2 = [makeNode({ urn: 'urn:li:document:target', title: 'Target', parentUrn: 'urn:li:document:a' })];
        let page = 0;

        const loadChildren = vi.fn(async () => {
            page = 1;
            page1.forEach((n) => nodes.set(n.urn, n));
            return page1;
        });
        const loadMoreChildren = vi.fn(async () => {
            page = 2;
            page2.forEach((n) => nodes.set(n.urn, n));
            return page2;
        });

        await revealDocumentInTree({
            documentUrn: 'urn:li:document:target',
            documentTitle: 'Target',
            parentDocuments: [{ urn: 'urn:li:document:a', title: 'A' }],
            getNode: (urn) => nodes.get(urn),
            expandNode,
            ensureNode,
            loadChildren,
            loadMoreChildren,
            hasMoreChildren: () => page < 2,
        });

        expect(loadChildren).toHaveBeenCalledTimes(1);
        expect(loadMoreChildren).toHaveBeenCalledTimes(1);
        expect(nodes.has('urn:li:document:target')).toBe(true);
    });

    it('pages through multiple child pages when the target is far down the list', async () => {
        const nodes = new Map<string, DocumentTreeNode>([
            ['urn:li:document:a', makeNode({ urn: 'urn:li:document:a', title: 'A', hasChildren: true })],
        ]);
        const ensureNode = vi.fn((node: DocumentTreeNode) => {
            nodes.set(node.urn, node);
        });
        const expandNode = vi.fn();
        // Simulate ~50 siblings (page size 25): target lands on page 3.
        const pages = [
            Array.from({ length: 25 }, (_, i) =>
                makeNode({ urn: `urn:li:document:p1-${i}`, parentUrn: 'urn:li:document:a' }),
            ),
            Array.from({ length: 25 }, (_, i) =>
                makeNode({ urn: `urn:li:document:p2-${i}`, parentUrn: 'urn:li:document:a' }),
            ),
            [makeNode({ urn: 'urn:li:document:target', title: 'Target', parentUrn: 'urn:li:document:a' })],
        ];
        let loadedPages = 0;

        const loadChildren = vi.fn(async () => {
            loadedPages = 1;
            pages[0].forEach((n) => nodes.set(n.urn, n));
            return pages[0];
        });
        const loadMoreChildren = vi.fn(async () => {
            const next = pages[loadedPages];
            loadedPages += 1;
            next.forEach((n) => nodes.set(n.urn, n));
            return next;
        });

        await revealDocumentInTree({
            documentUrn: 'urn:li:document:target',
            documentTitle: 'Target',
            parentDocuments: [{ urn: 'urn:li:document:a', title: 'A' }],
            getNode: (urn) => nodes.get(urn),
            expandNode,
            ensureNode,
            loadChildren,
            loadMoreChildren,
            hasMoreChildren: () => loadedPages < pages.length,
        });

        expect(loadChildren).toHaveBeenCalledTimes(1);
        expect(loadMoreChildren).toHaveBeenCalledTimes(2);
        expect(nodes.has('urn:li:document:target')).toBe(true);
    });
});
