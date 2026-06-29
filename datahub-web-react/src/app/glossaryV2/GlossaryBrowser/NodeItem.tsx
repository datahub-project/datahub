import { Pill } from '@components';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled, { useTheme } from 'styled-components/macro';

import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { SelectedMark } from '@app/glossaryV2/GlossaryBrowser/SelectedMark';
import TermItem from '@app/glossaryV2/GlossaryBrowser/TermItem';
import {
    TreeRowContainer,
    TreeRowIconSlot,
    TreeRowLeftContent,
    TreeRowTitle,
} from '@app/glossaryV2/GlossaryBrowser/treeRow.styles';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { resolveGlossaryEntityColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { getGlossaryEntityIcon } from '@app/glossaryV2/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Loader } from '@src/alchemy-components';
import useGlossaryChildren from '@src/app/entityV2/glossaryNode/useGlossaryChildren';

import { GlossaryNodeFragment } from '@graphql/fragments.generated';
import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

// --- Row chrome -------------------------------------------------------------
// `RowContainer` / `LeftContent` / `IconSlot` / `Title` live in
// `treeRow.styles.ts` (shared with `TermItem` so the two leaf-row types in
// the glossary tree stay visually identical). Caret button and right-content
// wrappers are local to this file since terms don't render them.

const ItemWrapper = styled.div`
    display: flex;
    flex-direction: column;
    position: relative;
`;

// Caret button is rendered inside the IconSlot when the node has children
// and is either expanded or hovered, swapping the colored glossary glyph
// for an expand/collapse affordance. Border-less + transparent so only the
// caret itself is visible.
const ExpandButton = styled.button`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 20px;
    height: 20px;
    padding: 0;
    border: none;
    background: transparent;
    cursor: pointer;
    color: inherit;

    &:hover {
        opacity: 0.7;
    }
`;

// Node rows render a child-count pill on the right when collapsed. Terms
// don't have children, so this stays local to NodeItem.
const RowContainer = styled(TreeRowContainer)`
    justify-content: space-between;
`;

const RightContent = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    margin-left: 8px;
    flex-shrink: 0;
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

    const { t: tc } = useTranslation('common.actions');
    const theme = useTheme();
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();
    const { entityData } = useGlossaryEntityData();

    const [areChildrenVisible, setAreChildrenVisible] = useState(false);
    const [isHovered, setIsHovered] = useState(false);

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

    function handleSelectNode() {
        if (selectNode) {
            const displayName = entityRegistry.getDisplayName(node.type, node);
            selectNode(node.urn, displayName);
        }
    }

    // Picker variant (AddRelatedTermsModal etc.) selects the node; otherwise
    // the row navigates to the node's entity page. Mirrors `DomainNode`'s
    // pattern so both sidebars feel identical when used as pickers vs. nav.
    function handleRowClick() {
        if (isSelecting) {
            handleSelectNode();
            return;
        }
        history.push(entityRegistry.getEntityUrl(node.type, node.urn));
    }

    function handleCaretClick(e: React.MouseEvent) {
        e.stopPropagation();
        setAreChildrenVisible((v) => !v);
    }

    const childNodes = children
        ?.filter((child) => child?.type === EntityType.GlossaryNode)
        .sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA, nodeB));
    const childTerms = children
        ?.filter((child) => child?.type === EntityType.GlossaryTerm)
        .sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA, termB));

    const isMultiSelected = isSelecting && selectedUrns?.includes(node.urn);
    const isOnEntityPage = entityData?.urn === node.urn;
    const isRowSelected = !!isOnEntityPage && !isSelecting;

    if (shouldHideNode) return null;

    // Route through the canonical resolver so the sidebar agrees with the entity header,
    // list cards, and modal picker on what color this node should render. `iconColor` is the
    // resolved color the parent NodeItem passed us during its own render; the resolver folds it
    // into the precedence chain as `inheritedColor`.
    const glossaryColor = resolveGlossaryEntityColor(node, generateColor, { inheritedColor: iconColor });
    const NodeIcon = getGlossaryEntityIcon(EntityType.GlossaryNode);

    const showCaret = hasChildren && (areChildrenVisible || isHovered);

    const renderLeadingGlyph = () => {
        if (showCaret) {
            const Caret = areChildrenVisible ? CaretDown : CaretRight;
            return (
                <ExpandButton
                    type="button"
                    onClick={handleCaretClick}
                    aria-expanded={areChildrenVisible}
                    aria-label={areChildrenVisible ? tc('collapse') : tc('expand')}
                    data-testid={`glossary-tree-expand-button-${node.urn}`}
                >
                    <Caret color={theme.colors.icon} size={16} weight="bold" />
                </ExpandButton>
            );
        }
        return <GlossaryColoredIcon color={glossaryColor} icon={NodeIcon} size={20} iconSize={12} />;
    };

    const displayName = entityRegistry.getDisplayName(node.type, node);

    return (
        <ItemWrapper>
            <RowContainer
                $level={depth}
                $isSelected={isRowSelected}
                onClick={handleRowClick}
                onMouseEnter={() => setIsHovered(true)}
                onMouseLeave={() => setIsHovered(false)}
                data-testid={`glossary-sidebar-node-${node.urn}`}
            >
                <TreeRowLeftContent>
                    <TreeRowIconSlot>{renderLeadingGlyph()}</TreeRowIconSlot>
                    <TreeRowTitle $isSelected={isRowSelected}>{displayName}</TreeRowTitle>
                </TreeRowLeftContent>
                {hasChildren && !areChildrenVisible && (
                    <RightContent>
                        <Pill label={`${noOfChildren}`} size="sm" />
                    </RightContent>
                )}
                {isMultiSelected && <SelectedMark />}
            </RowContainer>
            {areChildrenVisible && (
                <>
                    {!children.length && loading && (
                        <LoadingWrapper $level={depth + 1}>
                            <Loader size="xs" padding={0} />
                        </LoadingWrapper>
                    )}
                    {children.length > 0 && (
                        <ChildrenWrapper>
                            {(childNodes as GlossaryNode[]).map((child) => (
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
