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
    margin-top: 2px;
`;

type Props = {
    health: Health[];
    baseUrl: string;
    fontSize?: number;
    tooltipPlacement?: any;
    className?: string;
};

export const EntityHealth = ({ health, baseUrl, fontSize, tooltipPlacement, className }: Props) => {
    const unhealthy = isUnhealthy(health);
    const healthy = isHealthy(health);
    const icon = getHealthSummaryIcon(health, HealthSummaryIconType.FILLED, fontSize);
    return (
        <>
            {((unhealthy || healthy) && (
                <Link to={`${baseUrl}/Validation`} className={className}>
                    <Container>
                        <EntityHealthPopover health={health} baseUrl={baseUrl} placement={tooltipPlacement}>
                            {icon}
                        </EntityHealthPopover>
                    </Container>
                </Link>
            )) ||
                undefined}
        </>
    );
};
