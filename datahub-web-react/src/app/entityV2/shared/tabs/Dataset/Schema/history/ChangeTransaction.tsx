import React from 'react';
import styled from 'styled-components';
import { ChangeTransaction } from '../../../../../../../types.generated';
import { formatTimestamp } from './historyUtils';
import ChangeEventComponent from './ChangeEvent';

const ChangeTransactionTimestamp = styled.span`
    color: #56668e;
    font-size: 15px;
    font-style: normal;
    font-weight: 500;
    line-height: 16px; /* 160% */
    margin-left: 19px;
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
    padding-bottom: 24px;
`;

const ChangeEventCircle = styled.div`
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background-color: #56668e;
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
                <div>
                    <ChangeEventCircle />{' '}
                    <ChangeTransactionTimestamp>
                        {formatTimestamp(changeTransaction.timestampMillis)}
                    </ChangeTransactionTimestamp>
                </div>
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
