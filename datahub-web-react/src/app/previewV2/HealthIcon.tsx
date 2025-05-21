import { Popover, colors } from '@components';
import { CheckCircle, WarningCircle } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import HealthPopover from '@app/previewV2/HealthPopover';
import { isHealthy, isUnhealthy } from '@app/shared/health/healthUtils';

import { Health } from '@types';

const IconContainer = styled.div`
    display: flex;
    align-items: center;
`;

const UnhealthyIcon = styled(WarningCircle)`
    color: ${colors.red[500]};
    font-size: 20px;
`;

const HealthyIcon = styled(CheckCircle)`
    color: ${colors.green[500]};
    font-size: 20px;
`;

interface Props {
    urn: string;
    health: Health[];
    baseUrl: string;
    className?: string;
}

const HealthIcon = ({ urn, health, baseUrl, className }: Props) => {
    let icon: JSX.Element;
    if (isUnhealthy(health)) {
        icon = <UnhealthyIcon weight="regular" />;
    } else if (isHealthy(health)) {
        icon = <HealthyIcon weight="regular" />;
    } else {
        return null;
    }

    return (
        <Popover content={<HealthPopover health={health} baseUrl={baseUrl} />} placement="bottom" showArrow={false}>
            <IconContainer className={className} data-testid={`${urn}-health-icon`}>
                {icon}
            </IconContainer>
        </Popover>
    );
};

export default HealthIcon;
