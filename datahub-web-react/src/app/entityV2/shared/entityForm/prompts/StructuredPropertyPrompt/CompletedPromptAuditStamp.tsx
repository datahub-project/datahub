import { CheckCircle } from '@phosphor-icons/react/dist/csr/CheckCircle';
import { Typography } from 'antd';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

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
    color: ${(props) => props.theme.colors.text};
    font-size: 14px;
    font-family: Manrope;
    font-weight: 600;
    line-height: 18px;
    overflow: hidden;
    white-space: nowrap;
    display: flex;
`;

const AuditStampSubTitle = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    font-size: 12px;
    font-family: Manrope;
    font-weight: 500;
    line-height: 16px;
    word-wrap: break-word;
`;

const StyledCheckCircle = styled(CheckCircle).attrs({ size: 16, weight: 'fill' })`
    margin-right: 4px;
    color: ${(props) => props.theme.colors.iconSuccess};
`;

const AuditWrapper = styled.div`
    max-width: 95%;
`;

interface Props {
    completedByName: string;
    completedByTime: string;
}

export default function CompletedPromptAuditStamp({ completedByName, completedByTime }: Props) {
    const { t } = useTranslation('entity.form');

    return (
        <CompletedPromptContainer>
            <PadIcon>
                <StyledCheckCircle />
            </PadIcon>
            <AuditWrapper>
                <AuditStamp>
                    <Trans
                        t={t}
                        i18nKey="completedBy"
                        values={{ completedByName }}
                        components={{ name: <Typography.Text ellipsis={{ tooltip: completedByName }} /> }}
                    />
                </AuditStamp>
                <AuditStampSubTitle>{completedByTime}</AuditStampSubTitle>
            </AuditWrapper>
        </CompletedPromptContainer>
    );
}
