import { act } from '@testing-library/react';
import React from 'react';
import { describe, expect, it, vi } from 'vitest';

import TreeViewContextProvider from '@app/homeV3/modules/hierarchyViewModule/treeView/context/TreeViewContextProvider';
import useTreeViewContext from '@app/homeV3/modules/hierarchyViewModule/treeView/context/useTreeViewContext';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { renderHook } from '@utils/test-utils/renderHook';

import { EntityType } from '@types';

function asyncNode(overrides: Partial<TreeNode> = {}): TreeNode {
    return {
        value: 'urn:li:glossaryTerm:term',
        label: 'urn:li:glossaryTerm:term',
        hasAsyncChildren: true,
        entity: { urn: 'urn:li:glossaryTerm:term', type: EntityType.GlossaryTerm },
        ...overrides,
    };
}

function renderTreeContext(initialNode: TreeNode) {
    const loadChildren = vi.fn();
    // renderHook doesn't pass props to the wrapper, so the wrapper reads nodes from this closure.
    let nodes = [initialNode];
    const hook = renderHook(() => useTreeViewContext(), {
        wrapper: ({ children }: { children?: React.ReactNode }) => (
            <TreeViewContextProvider nodes={nodes} loadChildren={loadChildren}>
                {children}
            </TreeViewContextProvider>
        ),
    });
    const rerender = (props: { nodes: TreeNode[] }) => {
        nodes = props.nodes;
        hook.rerender();
    };
    return { ...hook, rerender, loadChildren };
}

describe('TreeViewContextProvider getIsExpandable', () => {
    it('stays expandable before its async children have been loaded, even with a zero count', () => {
        // A glossary node or domain whose own child count is 0 can still have related assets to load.
        const { result } = renderTreeContext(asyncNode({ totalChildren: 0 }));

        expect(result.current.getIsExpandable(asyncNode({ totalChildren: 0 }))).toBe(true);
    });

    it('stops being expandable once its async children loaded empty', () => {
        const { result, rerender, loadChildren } = renderTreeContext(asyncNode());

        act(() => result.current.expand(asyncNode()));
        expect(loadChildren).toHaveBeenCalledTimes(1);

        const loadedEmpty = asyncNode({ isChildrenLoading: false, totalChildren: 0, children: [] });
        rerender({ nodes: [loadedEmpty] });

        expect(result.current.getIsExpandable(loadedEmpty)).toBe(false);
    });

    it('stays expandable when the load failed and its child count is unknown', () => {
        const { result, rerender } = renderTreeContext(asyncNode());

        act(() => result.current.expand(asyncNode()));

        const loadFailed = asyncNode({ isChildrenLoading: false, totalChildren: undefined, children: [] });
        rerender({ nodes: [loadFailed] });

        expect(result.current.getIsExpandable(loadFailed)).toBe(true);
    });

    it('stays expandable while loading and when the load returned children', () => {
        const { result, rerender } = renderTreeContext(asyncNode());

        act(() => result.current.expand(asyncNode()));

        const loading = asyncNode({ isChildrenLoading: true, totalChildren: 0 });
        rerender({ nodes: [loading] });
        expect(result.current.getIsExpandable(loading)).toBe(true);

        const loadedWithChildren = asyncNode({ isChildrenLoading: false, totalChildren: 3 });
        rerender({ nodes: [loadedWithChildren] });
        expect(result.current.getIsExpandable(loadedWithChildren)).toBe(true);
    });
});
