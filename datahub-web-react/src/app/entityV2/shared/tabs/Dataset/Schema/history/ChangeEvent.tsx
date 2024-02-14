import React from 'react';
import styled from 'styled-components';
import { ChangeEvent } from '../../../../../../../types.generated';

const ChangeEventCircle = styled.div`
    display: inline-block;
    min-width: 6px;
    height: 6px;
    border-radius: 50%;
    border: 1px solid #56668e;
    margin-left: 1px;
    margin-top: 8px;
`;

const ChangeEventText = styled.div`
    display: inline-block;
    color: #56668e;
    font-family: 'Roboto Mono', monospace;
    font-size: 12px;
    font-style: normal;
    font-weight: 500;
    line-height: 24px; /* 200% */
    letter-spacing: -0.12px;
    margin-left: 22px;
`;

const ChangeEventContainer = styled.div`
    display: flex;
    flex-direction: row;
    width: 100%;
    margin-top: 8px;
`;

interface ChangeTransactionProps {
    changeEvent: ChangeEvent;
}

const ChangeEventComponent: React.FC<ChangeTransactionProps> = ({ changeEvent }) => {
    return (
        <ChangeEventContainer>
            <ChangeEventCircle /> <ChangeEventText>{changeEvent.description}</ChangeEventText>
        </ChangeEventContainer>
    );
};

export default ChangeEventComponent;
