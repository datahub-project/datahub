import { Form } from 'antd';
import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import ChildrenLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/ChildrenLoader';
import { ChildrenLoaderProvider } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/context/ChildrenLoaderProvider';
import useChildrenDomainsLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useChildrenDomainsLoader';
import { ChildrenLoaderMetadata } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import DomainSelectableTreeNodeRenderer from '@app/homeV3/modules/hierarchyViewModule/components/domains/DomainSelectableTreeNodeRenderer';
import useSelectableDomainTree from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useSelectableDomainTree';
import { useHierarchyFormContext } from '@app/homeV3/modules/hierarchyViewModule/components/form/HierarchyFormContext';
import TreeView from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeView';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';
import { getTopLevelSelectedValuesFromTree } from '@app/homeV3/modules/hierarchyViewModule/treeView/utils';

const Wrapper = styled.div``;

export default function DomainsSelectableTreeView() {
    const form = Form.useFormInstance();
    const {
        initialValues: { domainAssets: initialSelectedValues },
    } = useHierarchyFormContext();

    const [parentUrnsToFetchChildren, setParentUrnsToFetchChildren] = useState<string[]>([]);

    const { tree, selectedValues, setSelectedValues, loading } = useSelectableDomainTree(initialSelectedValues);

    const updateSelectedValues = useCallback(
        (newSelectedValues: string[]) => {
            const topLevelSelectedValues = getTopLevelSelectedValuesFromTree(newSelectedValues, tree.nodes);
            form.setFieldValue('domainAssets', topLevelSelectedValues);
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
                <ChildrenLoader parentValues={parentUrnsToFetchChildren} loadChildren={useChildrenDomainsLoader} />

                <TreeView
                    selectable
                    loading={loading}
                    nodes={tree.nodes}
                    explicitlySelectParent
                    selectedValues={selectedValues}
                    expandedValues={initialSelectedValues}
                    updateSelectedValues={updateSelectedValues}
                    loadChildren={startLoadingOfChildren}
                    renderNodeLabel={(nodeProps) => <DomainSelectableTreeNodeRenderer {...nodeProps} />}
                />
            </ChildrenLoaderProvider>
        </Wrapper>
    );
}
