import React, { useCallback } from 'react';
import styled from 'styled-components';

import EntityItem from '@app/homeV3/module/components/EntityItem';
import ChildrenLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/ChildrenLoader';
import { ChildrenLoaderProvider } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/context/ChildrenLoaderProvider';
import useChildrenDomainsLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useChildrenDomainsLoader';
import useDomainRelatedEntitiesLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useDomainRelatedEntitiesLoader';
import useParentValuesToLoadChildren from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useParentValues';
import { ChildrenLoaderMetadata } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import useDomainsTree from '@app/homeV3/modules/hierarchyViewModule/components/domains/hooks/useDomainsTree';
import TreeView from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeView';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

const Wrapper = styled.div``;

interface Props {
    assetUrns: string[];
    shouldShowRelatedEntities: boolean;
}

export default function DomainsTreeView({ assetUrns, shouldShowRelatedEntities }: Props) {
    const { tree, loading } = useDomainsTree(assetUrns ?? [], shouldShowRelatedEntities);
    const { parentValues, addParentValue, removeParentValue } = useParentValuesToLoadChildren();

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
            <ChildrenLoaderProvider onLoadFinished={onLoadFinished}>
                <ChildrenLoader
                    parentValues={parentValues}
                    loadChildren={useChildrenDomainsLoader}
                    loadRelatedEntities={shouldShowRelatedEntities ? useDomainRelatedEntitiesLoader : undefined}
                />

                <TreeView
                    loading={loading}
                    nodes={tree.nodes}
                    loadChildren={startLoadingOfChildren}
                    renderNodeLabel={(nodeProps) => (
                        <EntityItem entity={nodeProps.node.entity} hideSubtitle hideMatches padding="8px 13px 8px 0" />
                    )}
                />
            </ChildrenLoaderProvider>
        </Wrapper>
    );
}
