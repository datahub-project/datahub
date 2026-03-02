import { LoadingOutlined } from '@ant-design/icons';
import { colors } from '@components';
import { KeyboardArrowDownRounded, KeyboardArrowRightRounded } from '@mui/icons-material';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import { sortGlossaryNodes } from '@app/entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entityV2/glossaryTerm/utils';
import { useGlossaryEntityData } from '@app/entityV2/shared/GlossaryEntityContext';
import TermItem, { NameWrapper, TermLink as NodeLink } from '@app/glossaryV2/GlossaryBrowser/TermItem';
import { useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import { useEntityRegistry } from '@app/useEntityRegistry';
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
    overflow: ${(props) => !props.$isChildNode && 'hidden'};
`;

const NodeBadge = styled.span<{ color: string }>`
    position: absolute;
    height: 9px;
    width: 50px;
    background-color: ${({ color }) => color};
    top: 0;
    left: -15px;
    transform: rotate(-45deg);
    opacity: 1;
`;

const NodeWrapper = styled.div<{ $isSelected: boolean; $depth: number }>`
    align-items: center;
    display: flex;
    font-size: 16px;
    background-color: ${(props) => props.$isSelected && props.theme.colors.bgActive};
    padding-left: calc(${(props) => (props.$depth ? props.$depth * 18 + 12 : 12)}px);

    &:hover {
        background-color: ${colors.gray[100]};
        ${NameWrapper} {
            color: ${(props) => props.theme.colors.textBrand};
        }
    }
`;

const StyledRightOutlined = styled(KeyboardArrowRightRounded)<{ isSelected: boolean }>`
    color: ${(props) => (props.isSelected ? `${props.theme.colors.textHover}` : `${props.theme.colors.borderFocused}`)};
    cursor: pointer;
    margin-right: 6px;
    line-height: 0;
    :hover {
        stroke: ${(props) =>
            props.isSelected ? `${props.theme.colors.textHover}` : `${props.theme.colors.borderFocused}`};
    }
`;

const StyledDownOutlined = styled(KeyboardArrowDownRounded)<{ isSelected: boolean }>`
    color: ${(props) => (props.isSelected ? `${props.theme.colors.textHover}` : `${props.theme.colors.textActive}`)};
    cursor: pointer;
    margin-right: 6px;
    line-height: 0;
    :hover {
        stroke: ${(props) =>
            props.isSelected ? `${props.theme.colors.textHover}` : `${props.theme.colors.textActive}`};
    }
`;

const ChildrenWrapper = styled.div``;

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;

    svg {
        height: 15px;
        width: 15px;
    }
`;

const ChildrenCount = styled.div`
    padding: 0 8px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 20px;
    background-color: ${colors.gray[100]};
    color: ${colors.gray[1700]};
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

    if (shouldHideNode) return null;

    const glossaryColor = node.displayProperties?.colorHex || generateColor(node.urn);

    return (
        <ItemWrapper $isSelected={entityData?.urn === node.urn} $isChildNode={isChildNode}>
            {!isChildNode && <NodeBadge color={glossaryColor} />}
            <NodeWrapper $isSelected={entityData?.urn === node.urn} $depth={depth}>
                {areChildrenVisible && (
                    <StyledDownOutlined
                        fontSize="inherit"
                        viewBox="2 2 18 18"
                        onClick={() => setAreChildrenVisible(false)}
                        isSelected={entityData?.urn === node.urn}
                    />
                )}
                {!areChildrenVisible && (
                    <StyledRightOutlined
                        fontSize="inherit"
                        viewBox="2 2 18 18"
                        onClick={() => setAreChildrenVisible(true)}
                        isSelected={entityData?.urn === node.urn}
                    />
                )}
                {!isSelecting && (
                    <NodeLink
                        to={`${entityRegistry.getEntityUrl(node.type, node.urn)}`}
                        $isSelected={entityData?.urn === node.urn}
                        $areChildrenVisible={areChildrenVisible}
                        $isChildNode
                    >
                        {entityRegistry.getDisplayName(node.type, node)}
                    </NodeLink>
                )}
                {isSelecting && (
                    <NameWrapper showSelectStyles={!!selectNode} onClick={handleSelectNode}>
                        {entityRegistry.getDisplayName(node.type, node)}
                    </NameWrapper>
                )}
                {!!noOfChildren && <ChildrenCount>{noOfChildren}</ChildrenCount>}
            </NodeWrapper>
            <StyledDivider depth={depth} />
            {areChildrenVisible && (
                <>
                    {!children.length && loading && (
                        <LoadingWrapper>
                            <LoadingOutlined />
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
                                    isChildNode
                                    key={child.urn}
                                    depth={depth + 1}
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
