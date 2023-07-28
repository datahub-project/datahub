import React from 'react';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { Health } from '../../../../../../types.generated';
import { getHealthSummaryIcon, isUnhealthy } from '../../../../../shared/health/healthUtils';
import { EntityHealthPopover } from './EntityHealthPopover';

const Container = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
`;

type Props = {
    health: Health[];
    baseUrl: string;
};

export const EntityHealth = ({ health, baseUrl }: Props) => {
    const unhealthy = isUnhealthy(health);
    const icon = getHealthSummaryIcon(health);
    return (
        <>
            {(unhealthy && (
                <Link to={`${baseUrl}/Validation`}>
                    <Container>
                        <EntityHealthPopover health={health} baseUrl={baseUrl}>
                            {icon}
                        </EntityHealthPopover>
                    </Container>
                </Link>
            )) ||
                undefined}
        </>
    );
};
