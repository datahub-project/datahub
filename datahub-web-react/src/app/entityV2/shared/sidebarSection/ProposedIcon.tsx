import { Clock } from 'phosphor-react';
import React from 'react';
import styled from 'styled-components';

import { Tooltip, colors } from '@src/alchemy-components';

const StyledClock = styled(Clock)`
    margin-left: 5px;
    color: ${colors.gray[300]};
    align-self: center;
`;

interface Props {
    propertyName: string;
}

const ProposedIcon = ({ propertyName }: Props) => {
    return (
        <Tooltip overlay={`Proposed ${propertyName} - Pending Approval`}>
            <StyledClock size={12} />
        </Tooltip>
    );
};

export default ProposedIcon;
