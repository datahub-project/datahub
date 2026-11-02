import React, { useEffect, useState } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { SelectedMark } from '@app/glossaryV2/GlossaryBrowser/SelectedMark';
import TermItem from '@app/glossaryV2/GlossaryBrowser/TermItem';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { resolveGlossaryEntityColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { getGlossaryEntityIcon } from '@app/glossaryV2/utils';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { useTreeExpansionRegistry } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/TreeExpansionRegistry';
import {
    TREE_ROW_ENTITY_ICON_GLYPH_SIZE,
    TREE_ROW_ENTITY_ICON_SIZE,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Loader } from '@src/alchemy-components';
import useGlossaryChildren from '@src/app/entityV2/glossaryNode/useGlossaryChildren';

import { GlossaryNodeFragment } from '@graphql/fragments.generated';
import { EntityType, GlossaryTerm } from '@types';

const ItemWrapper = styled.div`
    display: flex;
    flex-direction: column;
    position: relative;
`;

const ChildrenWrapper = styled.div``;

const LoadingWrapper = styled.div<{ $level: number }>`
    padding: 4px 8px 4px ${(props) => 8 + props.$level * 16}px;
`;

interface Props {
    node: GlossaryNodeFragment;
    isSelecting?: boolean;
    hideTerms?: boolean;
    openToEntity?: boolean;
    refreshBrowser?: boolean;
    nodeUrnToHide?: string;
    selectTerm?: (urn: string, displayName: string) => void;
    selectNode?: (urn: string, displayName: string) => void;
    depth: number;
    selectedUrns?: string[];
    iconColor?: string;
}

function NodeItem(props: Props) {
    const {
        node,
        isSelecting,
        hideTerms,
        openToEntity,
        refreshBrowser,
        nodeUrnToHide,
        selectTerm,
        selectNode,
        depth,
        selectedUrns,
        iconColor,
    } = props;
    const shouldHideNode = nodeUrnToHide === node.urn;

    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();
    const { entityData } = useGlossaryEntityData();
    const expansion = useTreeExpansionRegistry();

    const [areChildrenVisible, setAreChildrenVisible] = useState(false);

    const entityUrn = node.urn;
    const {
        scrollRef,
        data: children,
        loading,
    } = useGlossaryChildren({ entityUrn, skip: !areChildrenVisible || shouldHideNode });

    useEffect(() => {
        if (openToEntity && entityData && entityData.parentNodes?.nodes?.some((parent) => parent.urn === node.urn)) {
            setAreChildrenVisible(true);
        }
    }, [entityData, node.urn, openToEntity]);

    useEffect(() => {
        if (refreshBrowser) {
            setAreChildrenVisible(false);
        }
    }, [refreshBrowser]);

    const noOfChildren = (node.childrenCount?.termsCount || 0) + (node.childrenCount?.nodesCount || 0);
    const hasChildren = noOfChildren > 0;

    useEffect(() => {
        if (!expansion || !hasChildren || shouldHideNode) return undefined;
        const api = {
            expand: () => setAreChildrenVisible(true),
            collapse: () => setAreChildrenVisible(false),
        };
        expansion.register(node.urn, api);
        return () => expansion.unregister(node.urn, api);
    }, [expansion, hasChildren, node.urn, shouldHideNode]);

    useEffect(() => {
        if (!expansion || !hasChildren || shouldHideNode) return;
        expansion.reportExpanded(node.urn, areChildrenVisible);
    }, [expansion, hasChildren, areChildrenVisible, node.urn, shouldHideNode]);

    function handleSelectNode() {
        if (selectNode) {
            const displayName = entityRegistry.getDisplayName(node.type, node);
            selectNode(node.urn, displayName);
        }
    }

    function handleRowClick() {
        if (isSelecting) {
            handleSelectNode();
            return;
        }
        history.push(entityRegistry.getEntityUrl(node.type, node.urn));
    }

    // Preserve scrollAcrossEntities order (type then name via sortInput). Do not re-sort.
    const childNodes = children?.filter((child) => child?.type === EntityType.GlossaryNode);
    const childTerms = children?.filter((child) => child?.type === EntityType.GlossaryTerm);

    const isMultiSelected = isSelecting && selectedUrns?.includes(node.urn);
    const isOnEntityPage = entityData?.urn === node.urn;
    const isRowSelected = !!isOnEntityPage && !isSelecting;

    if (shouldHideNode) return null;

    const glossaryColor = resolveGlossaryEntityColor(node, generateColor, { inheritedColor: iconColor });
    const NodeIcon = getGlossaryEntityIcon(EntityType.GlossaryNode);
    const displayName = entityRegistry.getDisplayName(node.type, node);

    return (
        <ItemWrapper>
            <HierarchicalBrowseTreeRow
                level={depth}
                isSelected={isRowSelected}
                hasChildren={hasChildren}
                isExpanded={areChildrenVisible}
                count={noOfChildren}
                icon={
                    <GlossaryColoredIcon
                        color={glossaryColor}
                        icon={NodeIcon}
                        size={TREE_ROW_ENTITY_ICON_SIZE}
                        iconSize={TREE_ROW_ENTITY_ICON_GLYPH_SIZE}
                    />
                }
                label={displayName}
                trailing={isMultiSelected ? <SelectedMark /> : undefined}
                onSelect={handleRowClick}
                onToggleExpand={() => setAreChildrenVisible((v) => !v)}
                data-testid={`glossary-sidebar-node-${node.urn}`}
            />
            {areChildrenVisible && (
                <>
                    {!children.length && loading && (
                        <LoadingWrapper $level={depth + 1}>
                            <Loader size="xs" padding={0} />
                        </LoadingWrapper>
                    )}
                    {children.length > 0 && (
                        <ChildrenWrapper>
                            {(childNodes as GlossaryNodeFragment[]).map((child) => (
                                <NodeItem
                                    node={child}
                                    isSelecting={isSelecting}
                                    hideTerms={hideTerms}
                                    openToEntity={openToEntity}
                                    nodeUrnToHide={nodeUrnToHide}
                                    selectTerm={selectTerm}
                                    selectNode={selectNode}
                                    key={child.urn}
                                    depth={depth + 1}
                                    selectedUrns={selectedUrns}
                                    iconColor={glossaryColor}
                                />
                            ))}
                            {!hideTerms &&
                                (childTerms as GlossaryTerm[]).map((child) => (
                                    <TermItem
                                        key={child.urn}
                                        term={child}
                                        isSelecting={isSelecting}
                                        selectTerm={selectTerm}
                                        includeActiveTabPath
                                        depth={depth + 1}
                                        selectedUrns={selectedUrns}
                                        iconColor={glossaryColor}
                                    />
                                ))}
                            <div ref={scrollRef} />
                        </ChildrenWrapper>
                    )}
                </>
            )}
        </ItemWrapper>
    );
}

export default NodeItem;
