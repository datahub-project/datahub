import React from 'react';
import { Popover } from 'antd';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import AmbulanceIcon from '../../images/ambulance-icon.svg?react';
import HealthPopover from './HealthPopover';
import { Health } from '../../types.generated';

const IconContainer = styled.div`
    display: flex;
    align-items: center;

    svg {
        height: 18px;
        width: auto;
    }
`;

interface Props {
    health: Health[];
    baseUrl: string;
}

const HealthIcon = ({ health, baseUrl }: Props) => {
    return (
        <Link to={`${baseUrl}/Incidents`}>
            <Popover content={<HealthPopover health={health} />} placement="bottom">
                <IconContainer>
                    <AmbulanceIcon />
                </IconContainer>
            </Popover>
        </Link>
    );
};

export default HealthIcon;
