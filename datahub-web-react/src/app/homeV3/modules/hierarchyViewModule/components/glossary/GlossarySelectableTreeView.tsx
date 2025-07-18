import { Form } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import ChildrenLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/ChildrenLoader';
import { ChildrenLoaderProvider } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/context/ChildrenLoaderProvider';
import useChildrenGlossaryLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useChildrenGlossarysLoader';
import { ChildrenLoaderMetadata } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import { useHierarchyFormContext } from '@app/homeV3/modules/hierarchyViewModule/components/form/HierarchyFormContext';
import { FORM_FIELD_GLOSSARY_ASSETS } from '@app/homeV3/modules/hierarchyViewModule/components/form/constants';
import GlossaryTreeNodeRenderer from '@app/homeV3/modules/hierarchyViewModule/components/glossary/GlossaryTreeNodeRenderer';
import useSelectableGlossaryTree from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useSelectableGlossaryTree';
import TreeView from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeView';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { getTopLevelSelectedValuesFromTree } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

const Wrapper = styled.div``;

export default function GlossarySelectableTreeView() {
    const form = Form.useFormInstance();
    const {
        initialValues: { glossaryAssets: initialSelectedValues },
    } = useHierarchyFormContext();

    const { tree, setSelectedValues, selectedValues, loading } = useSelectableGlossaryTree(initialSelectedValues ?? []);

    const [parentUrnsToFetchChildren, setParentUrnsToFetchChildren] = useState<string[]>([]);

    const updateSelectedValues = useCallback(
        (newSelectedValues: string[]) => {
            const topLevelSelectedValues = getTopLevelSelectedValuesFromTree(newSelectedValues, tree.nodes);
            form.setFieldValue(FORM_FIELD_GLOSSARY_ASSETS, topLevelSelectedValues);
            setSelectedValues(newSelectedValues);
        },
        [form, setSelectedValues, tree],
    );

    const onLoadFinished = useCallback(
        (newNodes: TreeNode[], metadata: ChildrenLoaderMetadata, parentDomainUrn: string) => {
            setParentUrnsToFetchChildren((prev) => prev.filter((value) => value !== parentDomainUrn));
            tree.update(newNodes, parentDomainUrn);
            tree.updateNode(parentDomainUrn, {
                isChildrenLoading: false,
                totalChildren: (metadata.totalNumberOfChildren ?? 0) + (metadata.totalNumberOfRelatedEntities ?? 0),
            });
        },
        [tree],
    );

    const startLoadingOfChildren = useCallback(
        (node: TreeNode) => {
            setParentUrnsToFetchChildren((prev) => [...new Set([...prev, node.value])]);
            tree.updateNode(node.value, { isChildrenLoading: true });
        },
        [tree],
    );

    return (
        <Wrapper>
            <ChildrenLoaderProvider onLoadFinished={onLoadFinished}>
                <ChildrenLoader parentValues={parentUrnsToFetchChildren} loadChildren={useChildrenGlossaryLoader} />

                <TreeView
                    selectable
                    explicitlySelectParent
                    loading={loading}
                    nodes={tree.nodes}
                    selectedValues={selectedValues}
                    expandedValues={initialSelectedValues}
                    updateSelectedValues={updateSelectedValues}
                    loadChildren={startLoadingOfChildren}
                    renderNodeLabel={(nodeProps) => <GlossaryTreeNodeRenderer {...nodeProps} />}
                />
            </ChildrenLoaderProvider>
        </Wrapper>
    );
}
