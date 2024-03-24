import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { EntityType } from '../../types.generated';
import EntityRegistry from '../entityV2/EntityRegistry';
import LineageStatusIcon from '../../images/lineage-status.svg?react';

const Container = styled.div<{ highlighted?: boolean }>`
    display: flex;
    & svg {
        height: 16px;
        width: 16px;
        color: ${({ highlighted }) => (highlighted ? '#3F54D1' : '#b0a2c2')} !important;
    }
`;

interface LineageBadgeProps {
    upstreamTotal: number | undefined;
    downstreamTotal: number | undefined;
    history: ReturnType<typeof useHistory>;
    entityRegistry: EntityRegistry;
    entityType: EntityType;
    urn: string;
}

const LineageBadge: React.FC<LineageBadgeProps> = ({
    upstreamTotal,
    downstreamTotal,
    history,
    entityRegistry,
    entityType,
    urn,
}) => (
    <Tooltip title={`${upstreamTotal} upstreams, ${downstreamTotal} downstreams`} showArrow={false}>
        <Container
            highlighted={(upstreamTotal || 0) + (downstreamTotal || 0) > 0}
            onClick={() => history.push(`${entityRegistry.getEntityUrl(entityType, urn)}/Lineage`)}
        >
            <LineageStatusIcon />
        </Container>
    </Tooltip>
);

export default LineageBadge;
