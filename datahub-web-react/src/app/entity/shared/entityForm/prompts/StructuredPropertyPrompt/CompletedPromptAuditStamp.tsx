import { Typography } from 'antd';
import React from 'react';
import Icon from '@ant-design/icons';
import styled from 'styled-components';
import GreenCircleIcon from '../../../../../../images/greenCircleTwoTone.svg?react';
import { ANTD_GRAY_V2 } from '../../../constants';

const PadIcon = styled.div`
    align-items: flex-start;
    padding-top: 1px;
    padding-right: 2px;
`;

const CompletedPromptContainer = styled.div`
    display: flex;
    align-self: end;
    max-width: 350px;
`;

const AuditStamp = styled.div`
    color: #373d44;
    font-size: 14px;
    font-family: Manrope;
    font-weight: 600;
    line-height: 18px;
    overflow: hidden;
    white-space: nowrap;
    display: flex;
`;

const AuditStampSubTitle = styled.div`
    color: ${ANTD_GRAY_V2[8]};
    font-size: 12px;
    font-family: Manrope;
    font-weight: 500;
    line-height: 16px;
    word-wrap: break-word;
`;

const StyledIcon = styled(Icon)`
    font-size: 16px;
    margin-right: 4px;
`;

const AuditWrapper = styled.div`
    max-width: 95%;
`;

interface Props {
    completedByName: string;
    completedByTime: string;
}

export default function CompletedPromptAuditStamp({ completedByName, completedByTime }: Props) {
    return (
        <CompletedPromptContainer>
            <PadIcon>
                <StyledIcon component={GreenCircleIcon} />
            </PadIcon>
            <AuditWrapper>
                <AuditStamp>
                    Completed by&nbsp;
                    <Typography.Text ellipsis={{ tooltip: completedByName }}>{completedByName}</Typography.Text>
                </AuditStamp>
                <AuditStampSubTitle>{completedByTime}</AuditStampSubTitle>
            </AuditWrapper>
        </CompletedPromptContainer>
    );
}
