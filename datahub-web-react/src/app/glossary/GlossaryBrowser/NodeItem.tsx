import { FolderOutlined, RightOutlined, DownOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import React, { useState, useEffect } from 'react';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { EntityType, GlossaryNode, GlossaryTerm } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { useGetGlossaryNodeQuery } from '../../../graphql/glossaryNode.generated';
import TermItem, { TermLink as NodeLink, NameWrapper } from './TermItem';
import { useEntityData } from '../../entity/shared/EntityContext';

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

const StyledFolderOutlined = styled(FolderOutlined)`
    margin-right: 6px;
`;

const ChildrenWrapper = styled.div`
    border-left: solid 1px ${ANTD_GRAY[4]};
    margin-left: 4px;
    padding-left: 12px;
`;

interface Props {
    node: GlossaryNode;
    isSelecting?: boolean;
    hideTerms?: boolean;
    openToEntity?: boolean;
    refreshBrowser?: boolean;
    nodeUrnToHide?: string;
    selectTerm?: (urn: string, displayName: string) => void;
    selectNode?: (urn: string, displayName: string) => void;
}

function NodeItem(props: Props) {
    const { node, isSelecting, hideTerms, openToEntity, refreshBrowser, nodeUrnToHide, selectTerm, selectNode } = props;
    const shouldHideNode = nodeUrnToHide === node.urn;

    const [areChildrenVisible, setAreChildrenVisible] = useState(false);
    const entityRegistry = useEntityRegistry();
    const { entityData } = useEntityData();
    const { data } = useGetGlossaryNodeQuery({
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
            .map((child) => child.entity) || [];
    const childTerms =
        (children as any)
            ?.filter((child) => child.entity?.type === EntityType.GlossaryTerm)
            .map((child) => child.entity) || [];

    if (shouldHideNode) return null;

    return (
        <ItemWrapper>
            <NodeWrapper>
                {!areChildrenVisible && <StyledRightOutlined onClick={() => setAreChildrenVisible(true)} />}
                {areChildrenVisible && <StyledDownOutlined onClick={() => setAreChildrenVisible(false)} />}
                {!isSelecting && (
                    <NodeLink
                        to={`/${entityRegistry.getPathName(node.type)}/${node.urn}`}
                        isSelected={entityData?.urn === node.urn}
                    >
                        <StyledFolderOutlined />
                        {entityRegistry.getDisplayName(node.type, isOnEntityPage ? entityData : node)}
                    </NodeLink>
                )}
                {isSelecting && (
                    <NameWrapper showSelectStyles={!!selectNode} onClick={handleSelectNode}>
                        <StyledFolderOutlined />
                        {entityRegistry.getDisplayName(node.type, isOnEntityPage ? entityData : node)}
                    </NameWrapper>
                )}
            </NodeWrapper>
            {areChildrenVisible && data && data.glossaryNode && (
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
                            <TermItem term={child} isSelecting={isSelecting} selectTerm={selectTerm} />
                        ))}
                </ChildrenWrapper>
            )}
        </ItemWrapper>
    );
}

export default NodeItem;
