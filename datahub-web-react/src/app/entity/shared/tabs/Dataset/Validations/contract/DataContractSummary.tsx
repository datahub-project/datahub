import React from 'react';
import styled from 'styled-components';
import { EditFilled } from '@ant-design/icons';
import { Button, Typography } from 'antd';
import { DataContractState } from '../../../../../../../types.generated';
import { AssertionStatusSummary } from '../types';
import { getContractSummaryIcon, getContractSummaryTitle, getContractSummaryMessage } from './utils';
import { ANTD_GRAY } from '../../../../constants';

const SummaryHeader = styled.div`
    width: 100%;
    padding-left: 40px;
    padding-top: 20px;
    padding-bottom: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
`;

const SummaryContainer = styled.div``;

const SummaryDescription = styled.div`
    display: flex;
    align-items: center;
`;

const SummaryMessage = styled.div`
    display: inline-block;
    margin-left: 20px;
`;

const SummaryTitle = styled(Typography.Title)`
    && {
        padding-bottom: 0px;
        margin-bottom: 0px;
    }
`;

const Actions = styled.div`
    margin: 12px;
    margin-right: 20px;
`;

const CreateButton = styled(Button)`
    display: flex;
    align-items: center;
    gap: 0.3rem;
    margin-right: 12px;
    border-color: ${(props) => props.theme.styles['primary-color']};
    color: ${(props) => props.theme.styles['primary-color']};
    letter-spacing: 2px;
    &&:hover {
        color: white;
        background-color: ${(props) => props.theme.styles['primary-color']};
        border-color: ${(props) => props.theme.styles['primary-color']};
    }
`;

const EditIconStyle = styled(EditFilled)`
    && {
        font-size: 12px;
    }
`;

type Props = {
    state: DataContractState;
    summary: AssertionStatusSummary;
    showContractBuilder: () => void;
};

export const DataContractSummary = ({ state, summary, showContractBuilder }: Props) => {
    const summaryIcon = getContractSummaryIcon(state, summary);
    const summaryTitle = getContractSummaryTitle(state, summary);
    const summaryMessage = getContractSummaryMessage(state, summary);
    return (
        <SummaryHeader>
            <SummaryContainer>
                <SummaryDescription>
                    {summaryIcon}
                    <SummaryMessage>
                        <SummaryTitle level={5}>{summaryTitle}</SummaryTitle>
                        <Typography.Text type="secondary">{summaryMessage}</Typography.Text>
                    </SummaryMessage>
                </SummaryDescription>
            </SummaryContainer>
            <Actions>
                <CreateButton onClick={showContractBuilder}>
                    <EditIconStyle />
                    EDIT
                </CreateButton>
            </Actions>
        </SummaryHeader>
    );
};
