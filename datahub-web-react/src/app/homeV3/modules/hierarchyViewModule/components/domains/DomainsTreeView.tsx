import React, { useCallback } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
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

import { AndFilterInput, DataHubPageModuleType } from '@types';

const Wrapper = styled.div``;

interface Props {
    assetUrns: string[];
    shouldShowRelatedEntities: boolean;
    relatedEntitiesOrFilters: AndFilterInput[] | undefined;
}

export default function DomainsTreeView({ assetUrns, shouldShowRelatedEntities, relatedEntitiesOrFilters }: Props) {
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

    const onExpand = useCallback((node: TreeNode) => {
        analytics.event({
            type: EventType.HomePageTemplateModuleExpandClick,
            assetUrn: node.entity.urn,
            moduleType: DataHubPageModuleType.Hierarchy,
        });
    }, []);

    return (
        <Wrapper>
            <ChildrenLoaderProvider onLoadFinished={onLoadFinished}>
                <ChildrenLoader
                    parentValues={parentValues}
                    loadChildren={useChildrenDomainsLoader}
                    loadRelatedEntities={shouldShowRelatedEntities ? useDomainRelatedEntitiesLoader : undefined}
                    relatedEntitiesOrFilters={relatedEntitiesOrFilters}
                />

                <TreeView
                    loading={loading}
                    nodes={tree.nodes}
                    loadChildren={startLoadingOfChildren}
                    onExpand={onExpand}
                    renderNodeLabel={(nodeProps) => (
                        <EntityItem
                            entity={nodeProps.node.entity}
                            hideSubtitle
                            hideMatches
                            padding="8px 13px 8px 0"
                            moduleType={DataHubPageModuleType.Hierarchy}
                        />
                    )}
                />
            </ChildrenLoaderProvider>
        </Wrapper>
    );
}
