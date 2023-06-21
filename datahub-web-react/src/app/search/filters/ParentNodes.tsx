import { FolderOpenOutlined } from '@ant-design/icons';
import { Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { EntityType, GlossaryNode, GlossaryTerm } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';

const NUM_VISIBLE_NODES = 2;

const ParentNodesWrapper = styled.div`
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
    display: flex;
    align-items: center;
    margin-bottom: 3px;
    max-width: 220px;
    overflow: hidden;
`;

const ParentNode = styled(Typography.Text)<{ color?: string }>`
    margin-left: 4px;
    color: ${(props) => (props.color ? props.color : ANTD_GRAY[7])};
`;

export const ArrowWrapper = styled.span`
    margin: 0 3px;
`;

interface Props {
    glossaryTerm: GlossaryTerm;
}

export default function ParentNodes({ glossaryTerm }: Props) {
    const entityRegistry = useEntityRegistry();

    const parentNodes: GlossaryNode[] = glossaryTerm.parentNodes?.nodes || [];
    // parent nodes are returned with direct parent first
    const orderedParentNodes = [...parentNodes].reverse();
    const visibleNodes = orderedParentNodes.slice(orderedParentNodes.length - NUM_VISIBLE_NODES);
    const numHiddenNodes = orderedParentNodes.length - NUM_VISIBLE_NODES;
    const includeNodePathTooltip = parentNodes.length > NUM_VISIBLE_NODES;

    if (!parentNodes.length) return null;

    return (
        <Tooltip
            overlayStyle={includeNodePathTooltip ? { maxWidth: 450 } : { display: 'none' }}
            placement="top"
            title={
                <>
                    {orderedParentNodes.map((glossaryNode, index) => (
                        <>
                            <FolderOpenOutlined />
                            <ParentNode color="white">
                                {entityRegistry.getDisplayName(EntityType.GlossaryNode, glossaryNode)}
                            </ParentNode>
                            {index !== orderedParentNodes.length - 1 && <ArrowWrapper>{'>'}</ArrowWrapper>}
                        </>
                    ))}
                </>
            }
        >
            <ParentNodesWrapper>
                {numHiddenNodes > 0 &&
                    [...Array(numHiddenNodes)].map(() => (
                        <>
                            <FolderOpenOutlined />
                            <ArrowWrapper>{'>'}</ArrowWrapper>
                        </>
                    ))}
                {visibleNodes.map((glossaryNode, index) => {
                    const displayName = entityRegistry.getDisplayName(EntityType.GlossaryNode, glossaryNode);
                    return (
                        <>
                            <FolderOpenOutlined />
                            <ParentNode ellipsis={!includeNodePathTooltip ? { tooltip: displayName } : true}>
                                {displayName}
                            </ParentNode>
                            {index !== visibleNodes.length - 1 && <ArrowWrapper>{'>'}</ArrowWrapper>}
                        </>
                    );
                })}
            </ParentNodesWrapper>
        </Tooltip>
    );
}
