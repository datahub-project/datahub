import React from 'react';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import ChangeEventComponent from '@app/entityV2/shared/tabs/Dataset/Schema/history/ChangeEvent';
import { formatTimestamp } from '@app/entityV2/shared/tabs/Dataset/Schema/history/historyUtils';
import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';

import { ChangeTransaction, DataPlatform } from '@types';

const TitleText = styled.span`
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-size: 13px;
    font-style: normal;
    font-weight: 600;
    line-height: 16px; /* 160% */
`;

const ChangeTransactionTimestamp = styled(TitleText)`
    background: #eeecfa;
    border-radius: 20px;
    padding: 5px 15px;
`;

const ActorText = styled.span`
    color: #8f94b2;
    font-size: 12px;
    font-style: italic;
    font-weight: 400;
`;

const ChangeTransactionContainer = styled.div`
    display: flex;
    flex-direction: row;
    width: 100%;
`;

const ChangeTransactionSidebar = styled.div`
    display: flex;
    flex-direction: column;
    width: 2px;
    margin-right: -2px;
    min-height: 100%;
`;

const ChangeTransactionMainContent = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
    min-height: 100%;
    padding-bottom: 36px;
`;

const ChangeTransactionTitle = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    margin-left: 15px;
`;

const TransactionDateHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
`;

const ChangeEventCircle = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 14px;
    height: 14px;
    border-radius: 50%;
    background-color: #d2d6e0;
    margin-left: -3px;
`;

const InnerEventCircle = styled.div`
    display: flex;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background-color: ${REDESIGN_COLORS.DARK_GREY};
`;

const ChangeEventVerticalLine = styled.div`
    width: 2px;
    height: 100%;
    margin-left: 3px;
    background-color: #e8e6eb;
`;

export interface ChangeTransactionEntry {
    transaction: ChangeTransaction;
    semanticVersion?: string;
    platform?: DataPlatform;
}

function extractActorName(actorUrn?: string | null): string | null {
    if (!actorUrn) return null;
    const parts = actorUrn.split(':');
    return parts[parts.length - 1] || null;
}

export default function ChangeTransactionView({ transaction, platform, semanticVersion }: ChangeTransactionEntry) {
    const actorName = extractActorName(transaction.actor);

    return (
        <ChangeTransactionContainer>
            <ChangeTransactionSidebar>
                <ChangeEventVerticalLine />
            </ChangeTransactionSidebar>
            <ChangeTransactionMainContent>
                <TransactionDateHeader>
                    <ChangeEventCircle>
                        <InnerEventCircle />
                    </ChangeEventCircle>
                    <ChangeTransactionTitle>
                        {platform && <PlatformIcon platform={platform} size={14} />}
                        <ChangeTransactionTimestamp>
                            {formatTimestamp(transaction.timestampMillis)}
                        </ChangeTransactionTimestamp>
                        {semanticVersion && <TitleText>{`(${semanticVersion})`}</TitleText>}
                        {actorName && <ActorText>by {actorName}</ActorText>}
                    </ChangeTransactionTitle>
                </TransactionDateHeader>
                <div>{transaction?.changes?.map((change) => <ChangeEventComponent changeEvent={change} />)}</div>
            </ChangeTransactionMainContent>
        </ChangeTransactionContainer>
    );
}
