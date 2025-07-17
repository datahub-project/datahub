import React, { useCallback, useState } from 'react';

import useDomainsTreeViewState from '../../form/sections/selectAssets/domainsTreeView/hooks/useDomainsTreeViewState';
import useInitialDomains from '../../form/sections/selectAssets/domainsTreeView/hooks/useInitialDomains';
import { TreeNode } from '../../treeView/types';
import { getTopLevelSelectedValuesFromTree, insertChildren } from '../../treeView/utils';
import styled from 'styled-components';
import AsyncDomainChildrenLoader from '../../form/sections/selectAssets/domainsTreeView/AsyncDomainChildrenLoader';
import TreeView from '../../treeView/TreeView';
import DomainTreeNodeRenderer from '../../form/sections/selectAssets/domainsTreeView/DomainTreeNodeRenderer';
import EntityItem from '@app/homeV3/module/components/EntityItem';

const Wrapper = styled.div``;


interface Props {
    assetUrns: string[];
}

export default function DomainsTreeView({ assetUrns }: Props) {
    const { nodes, setNodes, loading } = useDomainsTreeViewState(assetUrns ?? [], false, false);

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
            <AsyncDomainChildrenLoader
                parentDomainsUrns={parentUrnsToFetchChildren}
                onResponse={onAsyncLoaderResponse}
            />

            <TreeView
                loading={loading}
                nodes={nodes}
                explicitlySelectParent
                loadChildren={onLoadAsyncChildren}
                renderNodeLabel={(nodeProps) => <EntityItem entity={nodeProps.node.entity} hideSubtitle hideMatches />}
            />
        </Wrapper>
    );
}
