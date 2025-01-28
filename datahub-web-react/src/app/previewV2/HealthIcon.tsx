import React from 'react';
import { Popover } from '@components';
import styled from 'styled-components';
import { CheckCircleOutlined } from '@ant-design/icons';
import AmbulanceIcon from '../../images/ambulance-icon.svg?react';
import { isHealthy, isUnhealthy } from '../shared/health/healthUtils';
import { COLORS } from '../sharedV2/colors';
import HealthPopover from './HealthPopover';
import { Health } from '../../types.generated';

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
