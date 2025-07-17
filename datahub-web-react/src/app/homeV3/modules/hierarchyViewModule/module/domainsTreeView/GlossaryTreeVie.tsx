import React, { useCallback, useState } from 'react';

import { TreeNode } from '../../treeView/types';
import { insertChildren } from '../../treeView/utils';
import styled from 'styled-components';
import TreeView from '../../treeView/TreeView';
import AsyncGlossaryNodeChildrenLoader from '../../form/sections/selectAssets/glossaryTreeView/AsyncGlossaryNodeChildrenLoader';
import GlossaryTreeNodeRenderer from '../../form/sections/selectAssets/glossaryTreeView/GlossaryTreeNodeRenderer';
import useGlossaryTreeViewState from '../../form/sections/selectAssets/glossaryTreeView/hooks/useGlossaryTreeViewState';
import EntityItem from '@app/homeV3/module/components/EntityItem';

const Wrapper = styled.div``;


interface Props {
    assetUrns: string[];
}

export default function GlossaryTreeView({ assetUrns }: Props) {


    const { nodes, setNodes, loading } = useGlossaryTreeViewState(
        assetUrns ?? [], false,
    );

    const [parentUrnsToFetchChildren, setParentUrnsToFetchChildren] = useState<string[]>([]);

    const onAsyncLoaderResponse = useCallback(
        (newNodes: TreeNode[], parentDomainUrn: string) => {
            setParentUrnsToFetchChildren((prev) => prev.filter((value) => value !== parentDomainUrn));
            setNodes((prev) => insertChildren(prev, newNodes, parentDomainUrn));
        },
        [setNodes],
    );

    const onLoadAsyncChildren = useCallback((node: TreeNode) => {
        setParentUrnsToFetchChildren((prev) => [...prev, node.value]);
    }, []);


    return (
        <Wrapper>
            <AsyncGlossaryNodeChildrenLoader
                parentGlossaryNodesUrns={parentUrnsToFetchChildren}
                onResponse={onAsyncLoaderResponse}
            />

            <TreeView
                loading={loading}
                nodes={nodes}
                loadChildren={onLoadAsyncChildren}
                renderNodeLabel={(nodeProps) => <EntityItem entity={nodeProps.node.entity}  hideSubtitle hideMatches/>}
            />
        </Wrapper>
    );
}
