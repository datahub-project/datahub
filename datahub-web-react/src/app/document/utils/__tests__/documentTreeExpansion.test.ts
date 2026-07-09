import { describe, expect, it, vi } from 'vitest';

import { DocumentTreeNode } from '@app/document/DocumentTreeContext';
import {
    collectExpandableUrns,
    expandAllFolders,
    hasExpandedDescendant,
} from '@app/document/utils/documentTreeExpansion';

function makeNode(overrides: Partial<DocumentTreeNode> = {}): DocumentTreeNode {
    return {
        urn: 'urn:li:document:test',
        title: 'Test Document',
        parentUrn: null,
        hasChildren: false,
        children: undefined,
        isUnpublished: false,
        isExternal: false,
        platform: null,
        creator: null,
        ...overrides,
    };
}

describe('documentTreeExpansion', () => {
    describe('collectExpandableUrns', () => {
        it('returns only nodes that have children, depth-first, including nested folders', () => {
            const roots = [
                makeNode({
                    urn: 'folder-a',
                    hasChildren: true,
                    children: [
                        makeNode({ urn: 'leaf-a1' }),
                        makeNode({ urn: 'folder-a2', hasChildren: true, children: [makeNode({ urn: 'leaf-a2a' })] }),
                    ],
                }),
                makeNode({ urn: 'leaf-b' }),
            ];

            expect(collectExpandableUrns(roots)).toEqual(['folder-a', 'folder-a2']);
        });

        it('includes an expandable node whose children have not loaded yet', () => {
            const roots = [makeNode({ urn: 'folder', hasChildren: true, children: undefined })];
            expect(collectExpandableUrns(roots)).toEqual(['folder']);
        });

        it('returns an empty array when there are no folders', () => {
            expect(collectExpandableUrns([makeNode({ urn: 'leaf' })])).toEqual([]);
        });
    });

    describe('hasExpandedDescendant', () => {
        const roots = [makeNode({ urn: 'folder', hasChildren: true, children: [makeNode({ urn: 'leaf' })] })];

        it('is true when an expandable node is in the expanded set', () => {
            expect(hasExpandedDescendant(roots, new Set(['folder']))).toBe(true);
        });

        it('is false when no expandable node is expanded', () => {
            expect(hasExpandedDescendant(roots, new Set(['leaf']))).toBe(false);
            expect(hasExpandedDescendant(roots, new Set())).toBe(false);
        });
    });

    describe('expandAllFolders', () => {
        it('expands folders level-by-level, discovering deeper folders from loadChildren', async () => {
            // Tree: root-folder -> child-folder -> grandchild-leaf
            const childrenByUrn: Record<string, DocumentTreeNode[]> = {
                'root-folder': [makeNode({ urn: 'child-folder', hasChildren: true })],
                'child-folder': [makeNode({ urn: 'grandchild-leaf' })],
            };
            const loadChildren = vi.fn(async (urn: string) => childrenByUrn[urn] ?? []);
            const levels: string[][] = [];

            await expandAllFolders({
                roots: [makeNode({ urn: 'root-folder', hasChildren: true })],
                loadChildren,
                onExpandLevel: (urns) => levels.push(urns),
            });

            expect(levels).toEqual([['root-folder'], ['child-folder']]);
            expect(loadChildren).toHaveBeenCalledWith('root-folder');
            expect(loadChildren).toHaveBeenCalledWith('child-folder');
            expect(loadChildren).toHaveBeenCalledTimes(2);
        });

        it('does not revisit a folder reachable by more than one path', async () => {
            // Two roots both report the same shared folder as a child.
            const loadChildren = vi.fn(async (urn: string) => {
                if (urn === 'root-a' || urn === 'root-b') {
                    return [makeNode({ urn: 'shared-folder', hasChildren: true })];
                }
                return [];
            });
            const expanded: string[] = [];

            await expandAllFolders({
                roots: [makeNode({ urn: 'root-a', hasChildren: true }), makeNode({ urn: 'root-b', hasChildren: true })],
                loadChildren,
                onExpandLevel: (urns) => expanded.push(...urns),
            });

            expect(expanded).toEqual(['root-a', 'root-b', 'shared-folder']);
            expect(loadChildren).toHaveBeenCalledTimes(3);
        });

        it('does nothing when there are no folders to expand', async () => {
            const loadChildren = vi.fn(async () => []);
            const onExpandLevel = vi.fn();

            await expandAllFolders({ roots: [makeNode({ urn: 'leaf' })], loadChildren, onExpandLevel });

            expect(onExpandLevel).not.toHaveBeenCalled();
            expect(loadChildren).not.toHaveBeenCalled();
        });
    });
});
