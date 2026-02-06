import { StopOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import {
    StyledCheckOutlined,
    StyledClockCircleOutlined,
    StyledCloseOutlined,
    StyledExclamationOutlined,
} from '@app/entityV2/shared/tabs/Dataset/Validations/shared/styledComponents';

import { Assertion, AssertionStatus } from '@types';

const StatusContainer = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
`;

type Props = {
    assertion: Assertion;
};

export const DataContractAssertionStatus = ({ assertion }: Props) => {
    const { assertionStatus } = assertion;

    return (
        <StatusContainer>
            {!assertionStatus && <StopOutlined />}
            <Tooltip title="Assertion is passing">
                {assertionStatus === AssertionStatus.Passing && <StyledCheckOutlined />}
            </Tooltip>
            <Tooltip title="Assertion is failing">
                {assertionStatus === AssertionStatus.Failing && <StyledCloseOutlined />}
            </Tooltip>
            <Tooltip title="Assertion has completed with errors">
                {assertionStatus === AssertionStatus.Error && <StyledExclamationOutlined />}
            </Tooltip>
            <Tooltip title="Assertion is initializing">
                {assertionStatus === AssertionStatus.Init && <StyledClockCircleOutlined />}
            </Tooltip>
        </StatusContainer>
    );
};
