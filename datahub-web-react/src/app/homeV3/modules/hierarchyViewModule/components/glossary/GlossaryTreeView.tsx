import React, { useCallback, useState } from 'react';
import styled from 'styled-components';

import EntityItem from '@app/homeV3/module/components/EntityItem';
import ChildrenLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/ChildrenLoader';
import { ChildrenLoaderProvider } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/context/ChildrenLoaderProvider';
import useChildrenGlossaryLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useChildrenGlossarysLoader';
import useGlossaryRelatedEntitiesLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useGlossaryRelatedEntitiesLoader';
import { ChildrenLoaderMetadata } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import useGlossaryTree from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryTree';
import TreeView from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeView';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

const Wrapper = styled.div``;

interface Props {
    assetUrns: string[];
    shouldShowRelatedEntities: boolean;
}

export default function GlossaryTreeView({ assetUrns, shouldShowRelatedEntities }: Props) {
    const { tree, loading } = useGlossaryTree(assetUrns ?? []);

    const [parentUrnsToFetchChildren, setParentUrnsToFetchChildren] = useState<string[]>([]);

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
            setParentUrnsToFetchChildren((prev) => [...prev, node.value]);
            tree.updateNode(node.value, { isChildrenLoading: true });
        },
        [tree],
    );

    return (
        <Wrapper>
            <ChildrenLoaderProvider onLoadFinished={onLoadFinished}>
                <ChildrenLoader
                    parentValues={parentUrnsToFetchChildren}
                    loadChildren={useChildrenGlossaryLoader}
                    loadRelatedEntities={shouldShowRelatedEntities ? useGlossaryRelatedEntitiesLoader : undefined}
                />

                <TreeView
                    loading={loading}
                    nodes={tree.nodes}
                    loadChildren={startLoadingOfChildren}
                    renderNodeLabel={(nodeProps) => (
                        <EntityItem entity={nodeProps.node.entity} hideSubtitle hideMatches />
                    )}
                />
            </ChildrenLoaderProvider>
        </Wrapper>
    );
}
