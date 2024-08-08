import { FolderOpenOutlined } from '@ant-design/icons';
import { Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Entity } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';

const ParentNodesWrapper = styled.div`
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
    display: flex;
    align-items: center;
    margin-bottom: 3px;
    overflow: hidden;
`;

const ParentNode = styled(Typography.Text)<{ color?: string }>`
    margin-left: 4px;
    color: ${(props) => (props.color ? props.color : ANTD_GRAY[7])};
`;

export const ArrowWrapper = styled.span`
    margin: 0 3px;
`;

const StyledTooltip = styled(Tooltip)`
    display: flex;
    white-space: nowrap;
    overflow: hidden;
`;

const DEFAULT_NUM_VISIBLE = 2;

interface Props {
    parentEntities: Entity[];
    numVisible?: number;
}

export default function ParentEntities({ parentEntities, numVisible = DEFAULT_NUM_VISIBLE }: Props) {
    const entityRegistry = useEntityRegistry();

    // parent nodes/domains are returned with direct parent first
    const orderedParentEntities = [...parentEntities].reverse();
    const numHiddenEntities = orderedParentEntities.length - numVisible;
    const hasHiddenEntities = numHiddenEntities > 0;
    const visibleNodes = hasHiddenEntities ? orderedParentEntities.slice(numHiddenEntities) : orderedParentEntities;

    if (!parentEntities.length) return null;

    return (
        <StyledTooltip
            overlayStyle={hasHiddenEntities ? { maxWidth: 450 } : { display: 'none' }}
            placement="top"
            title={
                <>
                    {orderedParentEntities.map((parentEntity, index) => (
                        <>
                            <FolderOpenOutlined />
                            <ParentNode color="white">
                                {entityRegistry.getDisplayName(parentEntity.type, parentEntity)}
                            </ParentNode>
                            {index !== orderedParentEntities.length - 1 && <ArrowWrapper>{'>'}</ArrowWrapper>}
                        </>
                    ))}
                </>
            }
        >
            <ParentNodesWrapper>
                {hasHiddenEntities &&
                    [...Array(numHiddenEntities)].map(() => (
                        <>
                            <FolderOpenOutlined />
                            <ArrowWrapper>{'>'}</ArrowWrapper>
                        </>
                    ))}
                {visibleNodes.map((parentEntity, index) => {
                    const displayName = entityRegistry.getDisplayName(parentEntity.type, parentEntity);
                    return (
                        <>
                            <FolderOpenOutlined />
                            <ParentNode ellipsis={!hasHiddenEntities ? { tooltip: displayName } : true}>
                                {displayName}
                            </ParentNode>
                            {index !== visibleNodes.length - 1 && <ArrowWrapper>{'>'}</ArrowWrapper>}
                        </>
                    );
                })}
            </ParentNodesWrapper>
        </StyledTooltip>
    );
}
