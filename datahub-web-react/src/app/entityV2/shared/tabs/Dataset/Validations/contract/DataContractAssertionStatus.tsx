import { StopOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import {
    StyledCheckOutlined,
    StyledClockCircleOutlined,
    StyledCloseOutlined,
    StyledExclamationOutlined,
} from '@app/entityV2/shared/tabs/Dataset/Validations/shared/styledComponents';

import { Assertion, AssertionResultType } from '@types';

const StatusContainer = styled.div`
    width: 100%;
    display: flex;
    justify-content: center;
`;

type Props = {
    assertion: Assertion;
};

export const DataContractAssertionStatus = ({ assertion }: Props) => {
    const { t } = useTranslation('entity.profile.validations');
    const latestRun = (assertion.runEvents?.runEvents?.length && assertion.runEvents?.runEvents[0]) || undefined;
    const latestResultType = latestRun?.result?.type || undefined;

    return (
        <StatusContainer>
            {latestResultType === undefined && <StopOutlined />}
            <Tooltip title={t('assertionStatus.passing')}>
                {latestResultType === AssertionResultType.Success && <StyledCheckOutlined />}
            </Tooltip>
            <Tooltip title={t('assertionStatus.failing')}>
                {latestResultType === AssertionResultType.Failure && <StyledCloseOutlined />}
            </Tooltip>
            <Tooltip title={t('assertionStatus.completingWithErrors')}>
                {latestResultType === AssertionResultType.Error && <StyledExclamationOutlined />}
            </Tooltip>
            <Tooltip title={t('assertionStatus.initializing')}>
                {latestResultType === AssertionResultType.Init && <StyledClockCircleOutlined />}
            </Tooltip>
        </StatusContainer>
    );
};
