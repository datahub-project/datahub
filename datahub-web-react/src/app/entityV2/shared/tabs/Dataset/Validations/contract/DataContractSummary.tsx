import { Button, Heading, Text, Tooltip } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import React from 'react';
import { useTranslation } from 'react-i18next';
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

const SummaryTitle = styled(Heading)`
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
    margin-right: 12px;
    letter-spacing: 2px;
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
    const { t: tc } = useTranslation('common.actions');
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
                        <SummaryTitle type="h5">{summaryTitle}</SummaryTitle>
                        <Text type="span" color="textSecondary">
                            {summaryMessage}
                        </Text>
                    </SummaryMessage>
                </SummaryDescription>
            </SummaryContainer>
            <Actions>
                <Tooltip title={editDisabled ? editDisabledMessage : null}>
                    <CreateButton
                        variant="outline"
                        disabled={editDisabled}
                        onClick={showContractBuilder}
                        icon={{ icon: PencilSimple }}
                    >
                        {tc('edit')}
                    </CreateButton>
                </Tooltip>
            </Actions>
        </SummaryHeader>
    );
};
