import { Form } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import { useHierarchyFormContext } from '@app/homeV3/modules/hierarchyViewModule/form/HierarchyFormContext';
import { FORM_FIELD_GLOSSARY_ASSETS } from '@app/homeV3/modules/hierarchyViewModule/form/constants';
import AsyncGlossaryNodeChildrenLoader from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/AsyncGlossaryNodeChildrenLoader';
import GlossaryTreeNodeRenderer from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/GlossaryTreeNodeRenderer';
import useGlossaryTreeViewState from '@app/homeV3/modules/hierarchyViewModule/form/sections/selectAssets/glossaryTreeView/hooks/useGlossaryTreeViewState';
import TreeView from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeView';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import {
    getTopLevelSelectedValuesFromTree,
    insertChildren,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

const Wrapper = styled.div``;

export default function GlossaryTreeView() {
    const form = Form.useFormInstance();
    const {
        initialValues: { glossaryAssets: initialSelectedValues },
    } = useHierarchyFormContext();

    const { nodes, setNodes, setSelectedValues, selectedValues, loading } = useGlossaryTreeViewState(
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
            form.setFieldValue(FORM_FIELD_GLOSSARY_ASSETS, topLevelSelectedValues);
            setSelectedValues(newSelectedValues);
        },
        [form, setSelectedValues, nodes],
    );

    return (
        <Wrapper>
            <AsyncGlossaryNodeChildrenLoader
                parentGlossaryNodesUrns={parentUrnsToFetchChildren}
                onResponse={onAsyncLoaderResponse}
            />

            <TreeView
                selectable
                explicitlySelectParent
                loading={loading}
                nodes={nodes}
                selectedValues={selectedValues}
                expandedValues={initialSelectedValues}
                updateSelectedValues={updateSelectedValues}
                loadChildren={onLoadAsyncChildren}
                renderNodeLabel={(nodeProps) => <GlossaryTreeNodeRenderer {...nodeProps} />}
            />
        </Wrapper>
    );
}
