import React, { useState, useEffect } from 'react';
import { LoadingOutlined } from '@ant-design/icons';
import { KeyboardArrowDownRounded, KeyboardArrowRightRounded } from '@mui/icons-material';
import styled from 'styled-components/macro';
import { Entity, EntityType, GlossaryNode, GlossaryTerm } from '../../../types.generated';
import { REDESIGN_COLORS } from '../../entityV2/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetGlossaryNodeQuery } from '../../../graphql/glossaryNode.generated';
import TermItem, { TermLink as NodeLink, NameWrapper } from './TermItem';
import { sortGlossaryNodes } from '../../entityV2/glossaryNode/utils';
import { sortGlossaryTerms } from '../../entityV2/glossaryTerm/utils';
import { useGlossaryEntityData } from '../../entityV2/shared/GlossaryEntityContext';
import { generateColorFromPalette } from '../colorUtils';

interface ItemWrapperProps {
    isSelected: boolean;
}

const ItemWrapper = styled.div<ItemWrapperProps>`
    display: flex;
    flex-direction: column;
    font-weight: 700;
    padding: 11px;
    border-bottom: 1px solid ${REDESIGN_COLORS.BORDER_3};
    position: relative;
    overflow: hidden;
    background-color: ${(props) => props.isSelected && REDESIGN_COLORS.BACKGROUND_GRAY_2};
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

const NodeWrapper = styled.div`
    align-items: center;
    display: flex;
    font-size: 12px;
`;

const StyledRightOutlined = styled(KeyboardArrowRightRounded)`
    color: ${REDESIGN_COLORS.SECONDARY_LIGHT_GREY};
    cursor: pointer;
    margin-right: 6px;
    font-size: 10px;
    line-height: 0;
`;

const StyledDownOutlined = styled(KeyboardArrowDownRounded)`
    color: ${REDESIGN_COLORS.SECONDARY_LIGHT_GREY};
    cursor: pointer;
    margin-right: 6px;
    font-size: 10px;
    line-height: 0;
`;

const ChildrenWrapper = styled.div<{ hasNodes: boolean }>`
    margin-left: 16px;
    margin-top: 11px;
`;

const LoadingWrapper = styled.div`
    padding: 8px;
    display: flex;
    justify-content: center;

    svg {
        height: 15px;
        width: 15px;
    }
`;

interface Relationship {
    entity?: Entity | null;
}

interface Props {
    node: GlossaryNode;
    isSelecting?: boolean;
    hideTerms?: boolean;
    openToEntity?: boolean;
    refreshBrowser?: boolean;
    nodeUrnToHide?: string;
    selectTerm?: (urn: string, displayName: string) => void;
    selectNode?: (urn: string, displayName: string) => void;
    isChildNode?: boolean;
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
        if (openToEntity && entityData && entityData.parentNodes?.nodes.some((parent) => parent.urn === node.urn)) {
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
        <ItemWrapper isSelected={entityData?.urn === node.urn}>
            {!isChildNode && <NodeBadge color={glossaryColor} />}
            <NodeWrapper>
                {areChildrenVisible && (
                    <StyledDownOutlined
                        fontSize="inherit"
                        viewBox="2 2 18 18"
                        onClick={() => setAreChildrenVisible(false)}
                    />
                )}
                {!areChildrenVisible && (
                    <StyledRightOutlined
                        fontSize="inherit"
                        viewBox="2 2 18 18"
                        onClick={() => setAreChildrenVisible(true)}
                    />
                )}
                {!isSelecting && (
                    <NodeLink
                        to={`${entityRegistry.getEntityUrl(node.type, node.urn)}`}
                        isSelected={entityData?.urn === node.urn}
                        areChildrenVisible={areChildrenVisible}
                        isChildNode
                    >
                        {entityRegistry.getDisplayName(node.type, node)}
                    </NodeLink>
                )}
                {isSelecting && (
                    <NameWrapper showSelectStyles={!!selectNode} onClick={handleSelectNode}>
                        {entityRegistry.getDisplayName(node.type, node)}
                    </NameWrapper>
                )}
            </NodeWrapper>
            {areChildrenVisible && (
                <>
                    {!data && loading && (
                        <LoadingWrapper>
                            <LoadingOutlined />
                        </LoadingWrapper>
                    )}
                    {data && data.glossaryNode && (
                        <ChildrenWrapper hasNodes={!!childNodes?.length}>
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
                                />
                            ))}
                            {!hideTerms &&
                                (childTerms as GlossaryTerm[]).map((child) => (
                                    <TermItem
                                        term={child}
                                        isSelecting={isSelecting}
                                        selectTerm={selectTerm}
                                        includeActiveTabPath
                                        key={child.urn}
                                    />
                                ))}
                        </ChildrenWrapper>
                    )}
                </>
            )}
        </ItemWrapper>
    );
}

export default NodeItem;
