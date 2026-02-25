import { Tooltip } from '@components';
import EditIcon from '@mui/icons-material/Edit';
import { Button, Typography } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { AssertionStatusSummary } from '@app/entityV2/shared/tabs/Dataset/Validations/acrylTypes';
import {
    getContractSummaryIcon,
    getContractSummaryMessage,
    getContractSummaryTitle,
} from '@app/entityV2/shared/tabs/Dataset/Validations/contract/utils';

import { DataContractState } from '@types';

const SummaryHeader = styled.div`
    width: 100%;
    padding-left: 40px;
    padding-top: 20px;
    padding-bottom: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
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
    border-color: ${(props) => props.theme.colors.borderBrand};
    color: ${(props) => props.theme.colors.textBrand};
    letter-spacing: 2px;
    &&:hover {
        color: ${(props) => props.theme.colors.textOnFillBrand};
        background-color: ${(props) => props.theme.colors.buttonFillBrand};
        border-color: ${(props) => props.theme.colors.borderBrand};
    }
`;

const EditIconStyle = styled(EditIcon)`
    && {
        font-size: 16px;
    }
`;

type Props = {
    state: DataContractState;
    summary: AssertionStatusSummary;
    showContractBuilder: () => void;
    editDisabled?: boolean;
    editDisabledMessage?: React.ReactNode;
};

export const DataContractSummary = ({
    state,
    summary,
    showContractBuilder,
    editDisabled,
    editDisabledMessage,
}: Props) => {
    const theme = useTheme();
    const summaryIcon = getContractSummaryIcon(state, summary, theme.colors);
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
                <Tooltip title={editDisabled ? editDisabledMessage : null}>
                    <CreateButton disabled={editDisabled} onClick={showContractBuilder}>
                        <EditIconStyle />
                        EDIT
                    </CreateButton>
                </Tooltip>
            </Actions>
        </SummaryHeader>
    );
};
