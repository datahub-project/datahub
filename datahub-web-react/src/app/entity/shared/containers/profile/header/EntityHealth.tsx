import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { Health } from '../../../../../../types.generated';
import {
    getHealthSummaryIcon,
    HealthSummaryIconType,
    isHealthy,
    isUnhealthy,
} from '../../../../../shared/health/healthUtils';
import { EntityHealthPopover } from './EntityHealthPopover';

const Container = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

type Props = {
    urn: string;
    health: Health[];
    baseUrl: string;
    fontSize?: number;
    tooltipPlacement?: any;
};

export const EntityHealth = ({ urn, health, baseUrl, fontSize, tooltipPlacement }: Props) => {
    const unhealthy = isUnhealthy(health);
    const healthy = isHealthy(health);
    const icon = getHealthSummaryIcon(health, HealthSummaryIconType.FILLED, fontSize);

    return (
        <>
            {(unhealthy || healthy) && (
                <Link to={`${baseUrl}/Quality`}>
                    <Container>
                        <EntityHealthPopover
                            health={health.filter((h) => h.message)}
                            baseUrl={baseUrl}
                            placement={tooltipPlacement}
                        >
                            <span data-testid={`${urn}-health-icon`}>{icon}</span>
                        </EntityHealthPopover>
                    </Container>
                </Link>
            )}
        </>
    );
};
