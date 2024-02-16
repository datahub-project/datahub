import React from 'react';
import ReportProblemIcon from '@mui/icons-material/ReportProblem';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { Health } from '../../types.generated';
import { isHealthy, isUnhealthy } from '../shared/health/healthUtils';
import { EntityHealth } from '../entityV2/shared/containers/profile/header/EntityHealth';

const Container = styled.div<{ healthy?: boolean; unhealthy?: boolean }>`
    display: flex;
    & svg {
        font-size: 12px;
        color: ${({ healthy, unhealthy }) => {
            if (healthy) {
                return '#00b341';
            }
            if (unhealthy) {
                return '#d0021b';
            }
            return '#b0a2c2';
        }} !important;
    }
`;

interface HealthBadgeProps {
    health: Health[] | undefined;
    url: string;
}

const HealthBadge: React.FC<HealthBadgeProps> = ({ health, url }) => {
    const unhealthy = health && isUnhealthy(health);
    const healthy = health && isHealthy(health);

    return (
        <Container healthy={healthy} unhealthy={unhealthy}>
            {health && health.length > 0 && (healthy || unhealthy) ? (
                <EntityHealth baseUrl={url} health={health} />
            ) : (
                <Tooltip title="No health information available" showArrow={false}>
                    <ReportProblemIcon />
                </Tooltip>
            )}
        </Container>
    );
};

export default HealthBadge;
