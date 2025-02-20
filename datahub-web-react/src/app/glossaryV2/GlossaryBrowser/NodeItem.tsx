import React, { useState, useEffect } from 'react';
import { LoadingOutlined } from '@ant-design/icons';
import { KeyboardArrowDownRounded, KeyboardArrowRightRounded } from '@mui/icons-material';
import styled from 'styled-components/macro';
import { Entity, EntityType, GlossaryNode, GlossaryTerm } from '../../../types.generated';
import { GlossaryNodeFragment } from '../../../graphql/fragments.generated';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetGlossaryNodeQuery } from '../../../graphql/glossaryNode.generated';
import TermItem, { TermLink as NodeLink, NameWrapper } from './TermItem';
import { sortGlossaryNodes } from '../../entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '../../entityV2/glossaryTerm/utils';
import { useGlossaryEntityData } from '../../entityV2/shared/GlossaryEntityContext';
import { generateColorFromPalette } from '../colorUtils';

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
    padding: 13px 0;
    background-color: ${(props) => props.$isSelected && REDESIGN_COLORS.HIGHLIGHT_PURPLE};
    padding-left: calc(${(props) => (props.$depth ? props.$depth * 18 + 12 : 12)}px);
`;

const StyledRightOutlined = styled(KeyboardArrowRightRounded)<{ isSelected: boolean }>`
    color: ${(props) =>
        props.isSelected ? `${REDESIGN_COLORS.TITLE_PURPLE}` : `${REDESIGN_COLORS.SECONDARY_LIGHT_GREY}`};
    cursor: pointer;
    margin-right: 6px;
    line-height: 0;
    :hover {
        stroke: ${(props) =>
            props.isSelected ? `${REDESIGN_COLORS.TITLE_PURPLE}` : `${REDESIGN_COLORS.SECONDARY_LIGHT_GREY}`};
    }
`;

const StyledDownOutlined = styled(KeyboardArrowDownRounded)<{ isSelected: boolean }>`
    color: ${(props) => (props.isSelected ? `${REDESIGN_COLORS.TITLE_PURPLE}` : `${REDESIGN_COLORS.HOVER_PURPLE_2}`)};
    cursor: pointer;
    margin-right: 6px;
    line-height: 0;
    :hover {
        stroke: ${(props) =>
            props.isSelected ? `${REDESIGN_COLORS.TITLE_PURPLE}` : `${REDESIGN_COLORS.HOVER_PURPLE_2}`};
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
    padding: 1px 8px;
    display: flex;
    justify-content: center;
    border-radius: 10px;
    background-color: #eeecfa;
    color: #434863;
    font-size: 10px;
    font-weight: 400;
    margin-right: 13px;
`;

const StyledDivider = styled.div<{ depth: number }>`
    width: calc(100% + 26px + ${(props) => props.depth * 18}px);
    margin-left: calc(-13px - ${(props) => props.depth * 18}px);
    border-bottom: 1px solid #eae8fb;
`;

interface Relationship {
    entity?: Entity | null;
}

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

    const [areChildrenVisible, setAreChildrenVisible] = useState(false);
    const entityRegistry = useEntityRegistry();
    const { entityData, urnsToUpdate, setUrnsToUpdate } = useGlossaryEntityData();
    const { data, loading, refetch } = useGetGlossaryNodeQuery({
        variables: { urn: node.urn },
        skip: !areChildrenVisible || shouldHideNode,
    });

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

    useEffect(() => {
        if (urnsToUpdate.includes(node.urn)) {
            refetch();
            setUrnsToUpdate(urnsToUpdate.filter((urn) => urn !== node.urn));
        }
    });

    const children: Relationship[] | undefined = data?.glossaryNode?.children?.relationships;
    const noOfChildren = node.children?.total;

    function handleSelectNode() {
        if (selectNode) {
            const displayName = entityRegistry.getDisplayName(node.type, node);
            selectNode(node.urn, displayName);
        }
    }

    const childNodes =
        children
            ?.filter((child) => child.entity?.type === EntityType.GlossaryNode)
            .sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA.entity, nodeB.entity))
            .map((child) => child.entity) || [];
    const childTerms =
        children
            ?.filter((child) => child.entity?.type === EntityType.GlossaryTerm)
            .sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA.entity, termB.entity))
            .map((child) => child.entity) || [];

    if (shouldHideNode) return null;

    const glossaryColor = node.displayProperties?.colorHex || generateColorFromPalette(node.urn);

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
                    {!data && loading && (
                        <LoadingWrapper>
                            <LoadingOutlined />
                        </LoadingWrapper>
                    )}
                    {data && data.glossaryNode && (
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
                                    <>
                                        <TermItem
                                            term={child}
                                            isSelecting={isSelecting}
                                            selectTerm={selectTerm}
                                            includeActiveTabPath
                                            key={child.urn}
                                            depth={depth + 1}
                                        />
                                        <StyledDivider depth={depth + 1} />
                                    </>
                                ))}
                        </ChildrenWrapper>
                    )}
                </>
            )}
        </ItemWrapper>
    );
}

export default NodeItem;
