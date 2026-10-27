import { act, renderHook } from '@testing-library/react-hooks';
import React from 'react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { DocumentTreeNode, DocumentTreeProvider, useDocumentTree } from '@app/document/DocumentTreeContext';
import { useSectionExpansion } from '@app/document/hooks/useSectionExpansion';

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <DocumentTreeProvider>{children}</DocumentTreeProvider>
);

function makeNode(overrides: Partial<DocumentTreeNode> = {}): DocumentTreeNode {
    return { urn: 'urn:li:document:test', title: 'Test', parentUrn: null, hasChildren: false, ...overrides };
}

function deferred<T>() {
    let resolve!: (value: T) => void;
    const promise = new Promise<T>((res) => {
        resolve = res;
    });
    return { promise, resolve };
}

describe('useSectionExpansion', () => {
    let loadChildren: ReturnType<typeof vi.fn>;

    beforeEach(() => {
        loadChildren = vi.fn().mockResolvedValue([]);
    });

    const render = () =>
        renderHook(
            () => ({
                section: useSectionExpansion(loadChildren),
                tree: useDocumentTree(),
            }),
            { wrapper },
        );

    describe('isSectionExpanded', () => {
        it('reflects whether any folder in the section is expanded', () => {
            const roots = [makeNode({ urn: 'f', hasChildren: true })];
            const { result } = render();

            expect(result.current.section.isSectionExpanded(roots)).toBe(false);

            act(() => {
                result.current.tree.expandNode('f');
            });

            expect(result.current.section.isSectionExpanded(roots)).toBe(true);
        });
    });

    describe('toggleSectionExpandAll', () => {
        it('expands every folder, discovering deeper levels from loadChildren', async () => {
            const g = makeNode({ urn: 'g', hasChildren: true });
            loadChildren = vi.fn(async (urn: string) => (urn === 'f' ? [g] : []));
            const roots = [makeNode({ urn: 'f', hasChildren: true })];
            const { result } = render();

            await act(async () => {
                await result.current.section.toggleSectionExpandAll('sec', roots);
            });

            expect(result.current.tree.expandedUrns.has('f')).toBe(true);
            expect(result.current.tree.expandedUrns.has('g')).toBe(true);
            expect(loadChildren).toHaveBeenCalledWith('f');
            expect(loadChildren).toHaveBeenCalledWith('g');
        });

        it('collapses the whole section when something is already expanded', async () => {
            const roots = [
                makeNode({ urn: 'f', hasChildren: true, children: [makeNode({ urn: 'g', hasChildren: true })] }),
            ];
            const { result } = render();

            act(() => {
                result.current.tree.expandNode('f');
                result.current.tree.expandNode('g');
            });
            expect(result.current.tree.expandedUrns.size).toBe(2);

            await act(async () => {
                await result.current.section.toggleSectionExpandAll('sec', roots);
            });

            expect(result.current.tree.expandedUrns.has('f')).toBe(false);
            expect(result.current.tree.expandedUrns.has('g')).toBe(false);
            expect(loadChildren).not.toHaveBeenCalled();
        });

        it('marks the section in-flight while expanding and clears it when done', async () => {
            const d = deferred<DocumentTreeNode[]>();
            loadChildren = vi.fn(() => d.promise);
            const roots = [makeNode({ urn: 'f', hasChildren: true })];
            const { result } = render();

            let pending: Promise<void>;
            act(() => {
                pending = result.current.section.toggleSectionExpandAll('sec', roots);
            });

            expect(result.current.section.isSectionExpanding('sec')).toBe(true);

            await act(async () => {
                d.resolve([]);
                await pending;
            });

            expect(result.current.section.isSectionExpanding('sec')).toBe(false);
        });
    });
});
