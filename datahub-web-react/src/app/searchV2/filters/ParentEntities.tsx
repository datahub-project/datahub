import React from 'react';
import styled from 'styled-components';
import { FolderOpenOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { ContextPathSeparator } from '@src/app/previewV2/ContextPathSeparator';
import ContextPathEntityLink from '@src/app/previewV2/ContextPathEntityLink';
import { Entity } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { useEntityRegistry } from '../../useEntityRegistry';

const ParentNodesWrapper = styled.div`
    font-size: 12px;
    color: ${ANTD_GRAY[7]};
    display: flex;
    align-items: center;
    overflow: hidden;
    line-height: 20px;
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
    linksDisabled?: boolean; // don't allow links to parent entities
}

export default function ParentEntities({ parentEntities, numVisible = DEFAULT_NUM_VISIBLE, linksDisabled }: Props) {
    const entityRegistry = useEntityRegistry();

    // parent nodes/domains are returned with direct parent first
    const orderedParentEntities = [...parentEntities].reverse();
    const numHiddenEntities = orderedParentEntities.length - numVisible;
    const hasHiddenEntities = numHiddenEntities > 0;
    const visibleNodes = hasHiddenEntities ? orderedParentEntities.slice(numHiddenEntities) : orderedParentEntities;

    if (!parentEntities.length) return null;

    return (
        <StyledTooltip
            showArrow={false}
            overlayStyle={hasHiddenEntities ? { maxWidth: 450 } : { display: 'none' }}
            placement="top"
            title={
                <>
                    {orderedParentEntities.map((parentEntity, index) => (
                        <React.Fragment key={parentEntity.urn}>
                            <ContextPathEntityLink entity={parentEntity} />
                            {index !== orderedParentEntities.length - 1 && <ContextPathSeparator />}
                        </React.Fragment>
                    ))}
                </>
            }
        >
            <ParentNodesWrapper>
                {hasHiddenEntities &&
                    [...Array(numHiddenEntities)].map((index) => (
                        <React.Fragment key={`icons-${index || 0}`}>
                            <FolderOpenOutlined />
                            <ContextPathSeparator />
                        </React.Fragment>
                    ))}
                {visibleNodes.map((parentEntity, index) => {
                    const displayName = entityRegistry.getDisplayName(parentEntity.type, parentEntity);
                    return (
                        <React.Fragment key={displayName}>
                            <ContextPathEntityLink
                                key={parentEntity.urn}
                                entity={parentEntity}
                                linkDisabled={linksDisabled}
                                style={{ fontSize: '12px' }}
                            />
                            {index !== visibleNodes.length - 1 && <ContextPathSeparator />}
                        </React.Fragment>
                    );
                })}
            </ParentNodesWrapper>
        </StyledTooltip>
    );
}
