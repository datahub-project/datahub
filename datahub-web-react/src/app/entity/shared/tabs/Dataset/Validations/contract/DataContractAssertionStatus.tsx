import React from 'react';
import styled from 'styled-components';
import { Tooltip } from 'antd';
import { StopOutlined } from '@ant-design/icons';
import { Assertion, AssertionResultType } from '../../../../../../../types.generated';
import {
    StyledCheckOutlined,
    StyledClockCircleOutlined,
    StyledCloseOutlined,
    StyledExclamationOutlined,
} from '../shared/styledComponents';

const StatusContainer = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
`;

type Props = {
    assertion: Assertion;
};

export const DataContractAssertionStatus = ({ assertion }: Props) => {
    const latestRun = (assertion.runEvents?.runEvents?.length && assertion.runEvents?.runEvents[0]) || undefined;
    const latestResultType = latestRun?.result?.type || undefined;

    return (
        <StatusContainer>
            {latestResultType === undefined && <StopOutlined />}
            <Tooltip title="Assertion is passing">
                {latestResultType === AssertionResultType.Success && <StyledCheckOutlined />}
            </Tooltip>
            <Tooltip title="Assertion is failing">
                {latestResultType === AssertionResultType.Failure && <StyledCloseOutlined />}
            </Tooltip>
            <Tooltip title="Assertion has completed with errors">
                {latestResultType === AssertionResultType.Error && <StyledExclamationOutlined />}
            </Tooltip>
            <Tooltip title="Assertion is initializing">
                {latestResultType === AssertionResultType.Init && <StyledClockCircleOutlined />}
            </Tooltip>
        </StatusContainer>
    );
};
