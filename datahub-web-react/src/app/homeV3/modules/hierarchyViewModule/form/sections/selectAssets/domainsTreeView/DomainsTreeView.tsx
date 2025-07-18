import { Form } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { useHierarchyFormContext } from '@app/homeV3/modules/hierarchyViewModule/form/HierarchyFormContext';
import AsyncDomainChildrenLoader from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/AsyncDomainChildrenLoader';
import DomainTreeNodeRenderer from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/DomainTreeNodeRenderer';
import useDomainsTreeViewState from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/domainsTreeView/hooks/useDomainsTreeViewState';
import TreeView from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeView';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import {
    getTopLevelSelectedValuesFromTree,
    insertChildren,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

const Wrapper = styled.div``;

export default function DomainsTreeView() {
    const form = Form.useFormInstance();
    const {
        initialValues: { domainAssets: initialSelectedValues },
    } = useHierarchyFormContext();

    const { nodes, setNodes, selectedValues, setSelectedValues, loading } = useDomainsTreeViewState(
        initialSelectedValues ?? [],
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

    const updateSelectedValues = useCallback(
        (newSelectedValues: string[]) => {
            const topLevelSelectedValues = getTopLevelSelectedValuesFromTree(newSelectedValues, nodes);
            form.setFieldValue('domainAssets', topLevelSelectedValues);
            setSelectedValues(newSelectedValues);
        },
        [form, setSelectedValues, nodes],
    );

    return (
        <Wrapper>
            <AsyncDomainChildrenLoader
                parentDomainsUrns={parentUrnsToFetchChildren}
                onResponse={onAsyncLoaderResponse}
            />

            <TreeView
                selectable
                loading={loading}
                nodes={nodes}
                explicitlySelectParent
                selectedValues={selectedValues}
                expandedValues={initialSelectedValues}
                updateSelectedValues={updateSelectedValues}
                loadChildren={onLoadAsyncChildren}
                renderNodeLabel={(nodeProps) => <DomainTreeNodeRenderer {...nodeProps} />}
            />
        </Wrapper>
    );
}
