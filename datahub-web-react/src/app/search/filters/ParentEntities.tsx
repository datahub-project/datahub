import { FolderOpenOutlined } from '@ant-design/icons';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import { Tooltip, Typography } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity } from '@types';

const HIDDEN_COUNT_PREFIX = '+';

const ParentNodesWrapper = styled.div`
    font-size: 12px;
    color: ${(props) => props.theme.colors.textSecondary};
    display: flex;
    align-items: center;
    margin-bottom: 3px;
    overflow: hidden;
`;

const ParentNode = styled(Typography.Text)<{ color?: string }>`
    margin-left: 4px;
    color: ${(props) => (props.color ? props.color : props.theme.colors.textTertiary)};
`;

const ArrowWrapper = styled.span`
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
    const { t } = useTranslation('search');
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
                                {entityRegistry.getDisplayName(parentEntity.type, parentEntity) ||
                                    t('filters.unknownEntity')}
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
                            <>
                                {HIDDEN_COUNT_PREFIX}
                                {numHiddenEntities}
                            </>
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
                                {displayName || t('filters.unknownEntity')}
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
