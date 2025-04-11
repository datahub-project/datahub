import { FolderOpenOutlined } from '@ant-design/icons';
import { Tooltip, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { CaretRight } from 'phosphor-react';
import colors from '@src/alchemy-components/theme/foundations/colors';
import { Entity } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';

const ParentNodesWrapper = styled.div`
    font-size: 12px;
    color: ${colors.gray[1700]};
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
    display: flex;
    align-items: center;
`;

const StyledTooltip = styled(Tooltip)`
    display: flex;
    white-space: nowrap;
    overflow: hidden;
`;

const TooltipWrapper = styled.div`
    display: flex;
    align-items: center;
    flex-wrap: wrap;
`;

const DEFAULT_NUM_VISIBLE = 2;

interface Props {
    parentEntities: Entity[];
    numVisible?: number;
    hideIcon?: boolean;
}

export default function ParentEntities({ parentEntities, numVisible = DEFAULT_NUM_VISIBLE, hideIcon = false }: Props) {
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
                <TooltipWrapper>
                    {orderedParentEntities.map((parentEntity, index) => (
                        <>
                            {!hideIcon && <FolderOpenOutlined />}
                            <ParentNode color="white">
                                {entityRegistry.getDisplayName(parentEntity.type, parentEntity) || 'Unknown'}
                            </ParentNode>
                            {index !== orderedParentEntities.length - 1 && (
                                <ArrowWrapper>
                                    <CaretRight />
                                </ArrowWrapper>
                            )}
                        </>
                    ))}
                </TooltipWrapper>
            }
        >
            <ParentNodesWrapper>
                {hasHiddenEntities && (
                    <>
                        {!hideIcon ? (
                            [...Array(numHiddenEntities)].map(() => <FolderOpenOutlined />)
                        ) : (
                            <>+{numHiddenEntities}</>
                        )}
                        <ArrowWrapper>
                            <CaretRight />
                        </ArrowWrapper>
                    </>
                )}
                {visibleNodes.map((parentEntity, index) => {
                    const displayName = entityRegistry.getDisplayName(parentEntity.type, parentEntity);
                    const isLast = index === visibleNodes.length - 1;
                    return (
                        <>
                            {!hideIcon && <FolderOpenOutlined style={{ marginRight: 4 }} />}
                            <ParentNode
                                style={isLast ? { flexShrink: 1 } : { flexShrink: 2 }}
                                ellipsis={!hasHiddenEntities ? { tooltip: displayName } : true}
                            >
                                {displayName || 'Unknown'}
                            </ParentNode>
                            {!isLast && (
                                <ArrowWrapper>
                                    <CaretRight />
                                </ArrowWrapper>
                            )}
                        </>
                    );
                })}
            </ParentNodesWrapper>
        </StyledTooltip>
    );
}
