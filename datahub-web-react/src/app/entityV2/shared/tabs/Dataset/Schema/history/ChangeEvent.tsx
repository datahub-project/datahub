import { processDocumentationString } from '@src/app/lineageV2/lineageUtils';
import React from 'react';
import styled from 'styled-components';
import { ChangeEvent } from '../../../../../../../types.generated';
import { REDESIGN_COLORS } from '../../../../constants';
import { getDocumentationString } from './changeEventToString';

const ChangeEventCircle = styled.div`
    display: inline-block;
    min-width: 8px;
    height: 8px;
    border-radius: 50%;
    border: 1px solid ${REDESIGN_COLORS.DARK_GREY};
    margin-top: 8px;
`;

const ChangeEventText = styled.div`
    display: inline-block;
    color: #5f6685;
    font-size: 13px;
    font-style: normal;
    font-weight: 400;
    line-height: 20px; /* 200% */
    letter-spacing: -0.12px;
    margin-left: 22px;
    width: calc(100% - 22px);
`;

const ChangeEventContainer = styled.div`
    display: flex;
    flex-direction: row;
    width: 100%;
    margin-top: 8px;
    word-wrap: break-word;
`;

interface ChangeTransactionProps {
    changeEvent: ChangeEvent;
}

const ChangeEventComponent: React.FC<ChangeTransactionProps> = ({ changeEvent }) => {
    const documentationString = getDocumentationString(changeEvent);

    return (
        <ChangeEventContainer>
            <ChangeEventCircle /> <ChangeEventText>{processDocumentationString(documentationString)}</ChangeEventText>
        </ChangeEventContainer>
    );
};

export default ChangeEventComponent;
