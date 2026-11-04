import React, { useCallback } from 'react';
import styled from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import LifecycleStageBadge from '@app/entityV2/shared/containers/profile/header/LifecycleStageBadge';
import { VersionPill } from '@app/entityV2/shared/versioning/common';
import { usePageTemplateContext } from '@app/homeV3/context/PageTemplateContext';
import EntityItem from '@app/homeV3/module/components/EntityItem';
import ChildrenLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/ChildrenLoader';
import { ChildrenLoaderProvider } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/context/ChildrenLoaderProvider';
import useChildrenGlossaryLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useChildrenGlossarysLoader';
import useGlossaryRelatedEntitiesLoader from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useGlossaryRelatedEntitiesLoader';
import useParentValuesToLoadChildren from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/hooks/useParentValues';
import { ChildrenLoaderMetadata } from '@app/homeV3/modules/hierarchyViewModule/childrenLoader/types';
import useGlossaryTree from '@app/homeV3/modules/hierarchyViewModule/components/glossary/hooks/useGlossaryTree';
import { GlossaryTermType } from '@app/homeV3/modules/hierarchyViewModule/components/glossary/types';
import TreeView from '@app/homeV3/modules/hierarchyViewModule/treeView/TreeView';
import { TreeNode } from '@app/homeV3/modules/hierarchyViewModule/treeView/types';

import { AndFilterInput, DataHubPageModuleType, Entity, EntityType } from '@types';

const Wrapper = styled.div``;

const TermBadges = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
`;

interface Props {
    assetUrns: string[];
    shouldShowRelatedEntities: boolean;
    relatedEntitiesOrFilters: AndFilterInput[] | undefined;
}

export default function GlossaryTreeView({ assetUrns, shouldShowRelatedEntities, relatedEntitiesOrFilters }: Props) {
    const { tree, loading } = useGlossaryTree(assetUrns ?? [], shouldShowRelatedEntities);

    const { parentValues, addParentValue, removeParentValue } = useParentValuesToLoadChildren();
    const { templateType } = usePageTemplateContext();

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

    const onExpand = useCallback(
        (node: TreeNode) => {
            analytics.event({
                type: EventType.HomePageTemplateModuleExpandClick,
                assetUrn: node.entity.urn,
                moduleType: DataHubPageModuleType.Hierarchy,
                location: templateType,
            });
        },
        [templateType],
    );

    return (
        <Wrapper>
            <ChildrenLoaderProvider onLoadFinished={onLoadFinished}>
                <ChildrenLoader
                    parentValues={parentValues}
                    loadChildren={useChildrenGlossaryLoader}
                    loadRelatedEntities={shouldShowRelatedEntities ? useGlossaryRelatedEntitiesLoader : undefined}
                    relatedEntitiesOrFilters={relatedEntitiesOrFilters}
                />

                <TreeView
                    loading={loading}
                    nodes={tree.nodes}
                    loadChildren={startLoadingOfChildren}
                    onExpand={onExpand}
                    shouldExpandSingleRootNode
                    renderNodeLabel={(nodeProps) => (
                        <EntityItem
                            entity={nodeProps.node.entity}
                            hideSubtitle
                            hideMatches
                            padding="8px 13px 8px 0"
                            moduleType={DataHubPageModuleType.Hierarchy}
                            customHoverEntityName={(entity: Entity, nameContent: React.ReactNode) => {
                                if (entity.type !== EntityType.GlossaryTerm) return nameContent;
                                const term = entity as GlossaryTermType;
                                const versionTag = term.versionProperties?.version?.versionTag;
                                const lifecycleStage = term.status?.lifecycleStage;
                                if (!versionTag && !lifecycleStage) return nameContent;
                                return (
                                    <TermBadges>
                                        {nameContent}
                                        {!!versionTag && (
                                            <VersionPill
                                                label={versionTag}
                                                isLatest={term.versionProperties?.isLatest ?? false}
                                                size="xs"
                                            />
                                        )}
                                        <LifecycleStageBadge lifecycleStage={lifecycleStage} size="xs" />
                                    </TermBadges>
                                );
                            }}
                        />
                    )}
                />
            </ChildrenLoaderProvider>
        </Wrapper>
    );
}
