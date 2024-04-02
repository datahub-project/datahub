import React from 'react';
import styled from 'styled-components';
import { ChangeTransaction } from '../../../../../../../types.generated';
import { formatTimestamp } from './historyUtils';
import ChangeEventComponent from './ChangeEvent';
import { REDESIGN_COLORS } from '../../../../constants';

const ChangeTransactionTimestamp = styled.span`
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    font-size: 13px;
    font-style: normal;
    font-weight: 600;
    line-height: 16px; /* 160% */
    margin-left: 15px;
    background: #eeecfa;
    border-radius: 20px;
    padding: 5px 15px;
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

interface ChangeTransactionProps {
    changeTransaction: ChangeTransaction;
}

const ChangeTransactionComponent: React.FC<ChangeTransactionProps> = ({ changeTransaction }) => {
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
                    <ChangeTransactionTimestamp>
                        {formatTimestamp(changeTransaction.timestampMillis)}
                    </ChangeTransactionTimestamp>
                </TransactionDateHeader>
                <div>
                    {changeTransaction?.changes?.map((change) => (
                        <ChangeEventComponent changeEvent={change} />
                    ))}
                </div>
            </ChangeTransactionMainContent>
        </ChangeTransactionContainer>
    );
};

export default ChangeTransactionComponent;
