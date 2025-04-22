import { CheckCircleOutlined } from '@ant-design/icons';
import { Popover } from '@components';
import React from 'react';
import styled from 'styled-components';

import HealthPopover from '@app/previewV2/HealthPopover';
import { isHealthy, isUnhealthy } from '@app/shared/health/healthUtils';
import { COLORS } from '@app/sharedV2/colors';

import { Health } from '@types';

import AmbulanceIcon from '@images/ambulance-icon.svg?react';

const IconContainer = styled.div`
    display: flex;
    align-items: center;
    font-size: 112.5%;
`;

const HealthyIcon = styled(CheckCircleOutlined)`
    color: ${COLORS.green_6};
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
        icon = <AmbulanceIcon />;
    } else if (isHealthy(health)) {
        icon = <HealthyIcon />;
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
