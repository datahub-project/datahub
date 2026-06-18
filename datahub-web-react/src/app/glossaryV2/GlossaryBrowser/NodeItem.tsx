import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { BookmarksSimple } from '@phosphor-icons/react/dist/csr/BookmarksSimple';
import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import { SelectedMark } from '@app/glossaryV2/GlossaryBrowser/SelectedMark';
import TermItem, { NameWrapper, TermLink as NodeLink } from '@app/glossaryV2/GlossaryBrowser/TermItem';
import GlossaryColoredIcon from '@app/glossaryV2/GlossaryColoredIcon';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { Loader } from '@src/alchemy-components';
import useGlossaryChildren from '@src/app/entityV2/glossaryNode/useGlossaryChildren';

import { GlossaryNodeFragment } from '@graphql/fragments.generated';
import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

interface ItemWrapperProps {
    $isSelected: boolean;
    $isChildNode?: boolean;
}

const ItemWrapper = styled.div<ItemWrapperProps>`
    display: flex;
    flex-direction: column;
    font-weight: 700;
    position: relative;
`;

const StyledNodeIcon = styled(GlossaryColoredIcon)`
    margin-right: 8px;
`;

const NodeWrapper = styled.div<{ $isSelected: boolean; $depth: number }>`
    align-items: center;
    display: flex;
    font-size: 16px;
    background-color: ${(props) => props.$isSelected && props.theme.colors.bgActive};
    padding-left: calc(${(props) => (props.$depth ? props.$depth * 18 + 12 : 12)}px);

    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
        ${NameWrapper} {
            color: ${(props) => props.theme.colors.textBrand};
        }
    }
`;

const CaretSlot = styled.div`
    display: inline-flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    width: 14px;
    margin-right: 6px;
`;

const StyledCaretRight = styled(CaretRight)<{ $isSelected: boolean }>`
    color: ${(props) => (props.$isSelected ? props.theme.colors.iconSelected : props.theme.colors.icon)};
    cursor: pointer;
    line-height: 0;
    flex-shrink: 0;

    :hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

const StyledCaretDown = styled(CaretDown)<{ $isSelected: boolean }>`
    color: ${(props) => (props.$isSelected ? props.theme.colors.iconSelected : props.theme.colors.icon)};
    cursor: pointer;
    line-height: 0;
    flex-shrink: 0;

    :hover {
        color: ${(props) => props.theme.colors.iconHover};
    }
`;

const ChildrenWrapper = styled.div``;

const ChildrenCount = styled.div`
    padding: 0 8px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 20px;
    background-color: ${(props) => props.theme.colors.bgHover};
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    height: 22px;
    min-width: 28px;
    font-weight: 400;
    margin-right: 12px;
`;

const StyledDivider = styled.div<{ depth: number }>`
    width: calc(100% + 26px + ${(props) => props.depth * 18}px);
    margin-left: calc(-13px - ${(props) => props.depth * 18}px);
    border-bottom: 1px solid ${(props) => props.theme.colors.bgActive};
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
    isChildNode?: boolean;
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
        isChildNode,
        depth,
        selectedUrns,
        iconColor,
    } = props;
    const shouldHideNode = nodeUrnToHide === node.urn;

    const generateColor = useGenerateGlossaryColorFromPalette();
    const [areChildrenVisible, setAreChildrenVisible] = useState(false);
    const entityRegistry = useEntityRegistry();
    const entityUrn = node.urn;
    const {
        scrollRef,
        data: children,
        loading,
    } = useGlossaryChildren({ entityUrn, skip: !areChildrenVisible || shouldHideNode });
    const { entityData } = useGlossaryEntityData();

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

    function handleSelectNode() {
        if (selectNode) {
            const displayName = entityRegistry.getDisplayName(node.type, node);
            selectNode(node.urn, displayName);
        }
    }

    const childNodes = children
        ?.filter((child) => child?.type === EntityType.GlossaryNode)
        .sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA, nodeB));
    const childTerms = children
        ?.filter((child) => child?.type === EntityType.GlossaryTerm)
        .sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA, termB));

    const isSelected = isSelecting && selectedUrns?.includes(node.urn);

    if (shouldHideNode) return null;

    const glossaryColor = iconColor || node.displayProperties?.colorHex || generateColor(node.urn);
    const NodeIcon = iconColor ? BookmarkSimple : BookmarksSimple;

    return (
        <ItemWrapper $isSelected={entityData?.urn === node.urn} $isChildNode={isChildNode}>
            <NodeWrapper $isSelected={entityData?.urn === node.urn} $depth={depth}>
                <CaretSlot>
                    {noOfChildren > 0 &&
                        (areChildrenVisible ? (
                            <StyledCaretDown
                                size={14}
                                weight="regular"
                                onClick={() => setAreChildrenVisible(false)}
                                $isSelected={entityData?.urn === node.urn}
                            />
                        ) : (
                            <StyledCaretRight
                                size={14}
                                weight="regular"
                                onClick={() => setAreChildrenVisible(true)}
                                $isSelected={entityData?.urn === node.urn}
                            />
                        ))}
                </CaretSlot>
                <StyledNodeIcon color={glossaryColor} icon={NodeIcon} size={24} iconSize={14} />
                {!isSelecting && (
                    <NodeLink
                        to={`${entityRegistry.getEntityUrl(node.type, node.urn)}`}
                        $isSelected={entityData?.urn === node.urn}
                        $areChildrenVisible={areChildrenVisible}
                        $isChildNode
                        data-testid={`glossary-sidebar-node-${node.urn}`}
                    >
                        {entityRegistry.getDisplayName(node.type, node)}
                    </NodeLink>
                )}
                {isSelecting && (
                    <NameWrapper showSelectStyles={!!selectNode} onClick={handleSelectNode}>
                        {entityRegistry.getDisplayName(node.type, node)}
                    </NameWrapper>
                )}
                {isSelected && <SelectedMark />}
                {!!noOfChildren && <ChildrenCount>{noOfChildren}</ChildrenCount>}
            </NodeWrapper>
            <StyledDivider depth={depth} />
            {areChildrenVisible && (
                <>
                    {!children.length && loading && <Loader size="xs" padding={8} />}
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
                                    isChildNode
                                    key={child.urn}
                                    depth={depth + 1}
                                    selectedUrns={selectedUrns}
                                    iconColor={glossaryColor}
                                />
                            ))}
                            {!hideTerms &&
                                (childTerms as GlossaryTerm[]).map((child) => (
                                    <span key={child.urn}>
                                        <TermItem
                                            term={child}
                                            isSelecting={isSelecting}
                                            selectTerm={selectTerm}
                                            includeActiveTabPath
                                            depth={depth + 1}
                                            selectedUrns={selectedUrns}
                                            iconColor={glossaryColor}
                                        />
                                        <StyledDivider depth={depth + 1} />
                                    </span>
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
