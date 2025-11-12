import { Form } from 'antd';
import React, { useCallback } from 'react';
import styled from 'styled-components';

import ChildrenLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/ChildrenLoader';
import { ChildrenLoaderProvider } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/context/ChildrenLoaderProvider';
import useChildrenDomainsLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useChildrenDomainsLoader';
import useParentValuesToLoadChildren from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useParentValues';
import { ChildrenLoaderMetadata } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import DomainSelectableTreeNodeRenderer from '@app/homeV3/modules/hierarchyViewModule/components/domains/DomainSelectableTreeNodeRenderer';
import useSelectableDomainTree from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useSelectableDomainTree';
import { useHierarchyFormContext } from '@app/homeV3/modules/hierarchyViewModule/components/form/HierarchyFormContext';
import TreeView from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeView';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import {
    getOutOfTreeSelectedValues,
    getTopLevelSelectedValuesFromTree,
} from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

const Wrapper = styled.div``;

const LOAD_BATCH_SIZE = 25;

export default function DomainsSelectableTreeView() {
    const form = Form.useFormInstance();
    const {
        initialValues: { domainAssets: initialSelectedValues },
    } = useHierarchyFormContext();

    const { parentValues, addParentValue, removeParentValue } = useParentValuesToLoadChildren();

    const {
        tree,
        selectedValues,
        setSelectedValues,
        loading,
        loadMoreRootNodes,
        rootDomainsMoreLoading,
        rootNodesTotal,
    } = useSelectableDomainTree(initialSelectedValues, LOAD_BATCH_SIZE);

    const updateSelectedValues = useCallback(
        (newSelectedValues: string[]) => {
            const topLevelSelectedValues = getTopLevelSelectedValuesFromTree(newSelectedValues, tree.nodes);
            // add out of tree selected values as some root nodes could not be loaded yet
            const outOfTreeSelectedValues = getOutOfTreeSelectedValues(newSelectedValues, tree.nodes);
            const newProcessedSelectedValues = new Set([...topLevelSelectedValues, ...outOfTreeSelectedValues]);
            form.setFieldValue('domainAssets', Array.from(newProcessedSelectedValues));
            setSelectedValues(newSelectedValues);
        },
        [form, setSelectedValues, tree],
    );

    const onLoadFinished = useCallback(
        (newNodes: TreeNode[], metadata: ChildrenLoaderMetadata, parentDomainUrn: string) => {
            removeParentValue(parentDomainUrn);
            tree.update(newNodes, parentDomainUrn);
            tree.updateNode(parentDomainUrn, {
                isChildrenLoading: false,
                totalChildren: (metadata.totalNumberOfChildren ?? 0) + (metadata.totalNumberOfRelatedEntities ?? 0),
            });
        },
        [tree, removeParentValue],
    );

    const startLoadingOfChildren = useCallback(
        (node: TreeNode) => {
            addParentValue(node.value);
            tree.updateNode(node.value, { isChildrenLoading: true });
        },
        [tree, addParentValue],
    );

    return (
        <Wrapper>
            <ChildrenLoaderProvider onLoadFinished={onLoadFinished} maxNumberOfChildrenToLoad={LOAD_BATCH_SIZE}>
                <ChildrenLoader parentValues={parentValues} loadChildren={useChildrenDomainsLoader} />

                <TreeView
                    selectable
                    loading={loading}
                    nodes={tree.nodes}
                    explicitlySelectParent
                    expandParentNodesOfInitialSelectedValues
                    selectedValues={selectedValues}
                    updateSelectedValues={updateSelectedValues}
                    loadChildren={startLoadingOfChildren}
                    renderNodeLabel={(nodeProps) => <DomainSelectableTreeNodeRenderer {...nodeProps} />}
                    loadRootNodes={loadMoreRootNodes}
                    rootNodesLoading={rootDomainsMoreLoading}
                    loadingTriggerType="infiniteScroll"
                    rootNodesTotal={rootNodesTotal}
                    loadBatchSize={LOAD_BATCH_SIZE}
                />
            </ChildrenLoaderProvider>
        </Wrapper>
    );
}
