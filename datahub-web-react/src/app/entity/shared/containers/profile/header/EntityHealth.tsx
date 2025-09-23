import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { EntityHealthPopover } from '@app/entity/shared/containers/profile/header/EntityHealthPopover';
import { HealthSummaryIconType, getHealthSummaryIcon, isUnhealthy } from '@app/shared/health/healthUtils';

import { Health } from '@types';

const Container = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

type Props = {
    health: Health[];
    baseUrl: string;
    fontSize?: number;
    tooltipPlacement?: any;
};

export const EntityHealth = ({ health, baseUrl, fontSize, tooltipPlacement }: Props) => {
    const unhealthy = isUnhealthy(health);
    const icon = getHealthSummaryIcon(health, HealthSummaryIconType.FILLED, fontSize);
    return (
        <>
            {(unhealthy && (
                <Link to={`${baseUrl}/Quality`}>
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
