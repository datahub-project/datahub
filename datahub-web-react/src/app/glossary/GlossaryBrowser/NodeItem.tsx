import { DownOutlined, LoadingOutlined, RightOutlined } from '@ant-design/icons';
import { BookmarksSimple } from '@phosphor-icons/react';
import React, { useEffect, useState } from 'react';
import styled from 'styled-components/macro';

import { sortGlossaryNodes } from '@app/entity/glossaryNode/utils';
import { sortGlossaryTerms } from '@app/entity/glossaryTerm/utils';
import { useGlossaryEntityData } from '@app/entity/shared/GlossaryEntityContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import TermItem, { NameWrapper, TermLink as NodeLink } from '@app/glossary/GlossaryBrowser/TermItem';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetGlossaryNodeQuery } from '@graphql/glossaryNode.generated';
import { EntityType, GlossaryNode, GlossaryTerm } from '@types';

const ItemWrapper = styled.div`
    display: flex;
    flex-direction: column;
    font-weight: 700;
    padding-left: 4px;
`;

const NodeWrapper = styled.div`
    align-items: center;
    display: flex;
    margin-bottom: 4px;
`;

const StyledRightOutlined = styled(RightOutlined)`
    cursor: pointer;
    margin-right: 6px;
    font-size: 10px;
`;

const StyledDownOutlined = styled(DownOutlined)`
    cursor: pointer;
    margin-right: 6px;
    font-size: 10px;
`;

const StyledGlossaryNodeIcon = styled(BookmarksSimple)`
    margin-right: 6px;
`;

const ChildrenWrapper = styled.div`
    border-left: solid 1px ${ANTD_GRAY[4]};
    margin-left: 4px;
    padding-left: 12px;
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

interface Props {
    node: GlossaryNode;
    isSelecting?: boolean;
    hideTerms?: boolean;
    openToEntity?: boolean;
    refreshBrowser?: boolean;
    nodeUrnToHide?: string;
    termUrnToHide?: string;
    selectTerm?: (urn: string, displayName: string) => void;
    selectNode?: (urn: string, displayName: string) => void;
}

function NodeItem(props: Props) {
    const {
        node,
        isSelecting,
        hideTerms,
        openToEntity,
        refreshBrowser,
        nodeUrnToHide,
        termUrnToHide,
        selectTerm,
        selectNode,
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

    const isOnEntityPage = entityData && entityData.urn === node.urn;

    const children =
        entityData && isOnEntityPage ? entityData.children?.relationships : data?.glossaryNode?.children?.relationships;

    function handleSelectNode() {
        if (selectNode) {
            const displayName = entityRegistry.getDisplayName(node.type, node);
            selectNode(node.urn, displayName);
        }
    }

    const childNodes =
        (children as any)
            ?.filter((child) => child.entity?.type === EntityType.GlossaryNode)
            .sort((nodeA, nodeB) => sortGlossaryNodes(entityRegistry, nodeA.entity, nodeB.entity))
            .map((child) => child.entity) || [];
    const childTerms =
        (children as any)
            ?.filter((child) => child.entity?.type === EntityType.GlossaryTerm)
            .sort((termA, termB) => sortGlossaryTerms(entityRegistry, termA.entity, termB.entity))
            .map((child) => child.entity) || [];

    if (shouldHideNode) return null;

    return (
        <ItemWrapper>
            <NodeWrapper>
                {!areChildrenVisible && <StyledRightOutlined onClick={() => setAreChildrenVisible(true)} />}
                {areChildrenVisible && <StyledDownOutlined onClick={() => setAreChildrenVisible(false)} />}
                {!isSelecting && (
                    <NodeLink
                        to={`${entityRegistry.getEntityUrl(node.type, node.urn)}`}
                        $isSelected={entityData?.urn === node.urn}
                    >
                        <StyledGlossaryNodeIcon />
                        {entityRegistry.getDisplayName(node.type, isOnEntityPage ? entityData : node)}
                    </NodeLink>
                )}
                {isSelecting && (
                    <NameWrapper showSelectStyles={!!selectNode} onClick={handleSelectNode}>
                        <StyledGlossaryNodeIcon />
                        {entityRegistry.getDisplayName(node.type, isOnEntityPage ? entityData : node)}
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
                                />
                            ))}
                            {!hideTerms &&
                                (childTerms as GlossaryTerm[]).map((child) => (
                                    <TermItem
                                        term={child}
                                        isSelecting={isSelecting}
                                        selectTerm={selectTerm}
                                        includeActiveTabPath
                                        termUrnToHide={termUrnToHide}
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
