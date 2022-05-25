import React from 'react';
import styled from 'styled-components';
import { BookOutlined } from '@ant-design/icons';

type Props = {
    suggestion: string;
};

const SuggestionContainer = styled.div`
    display: 'flex',
    flex-direction: 'row',
    align-items: 'center',
`;

const SuggestionText = styled.span`
    margin-left: 2px;
    font-size: 10px;
    line-height: 20px;
    white-space: nowrap;
    margin-right: 5px;
    opacity: 1;
    color: #434343;
`;

export default function TermPill({ suggestion }: Props) {
    return (
        <SuggestionContainer>
            <BookOutlined style={{ marginRight: '2px' }} />
            <SuggestionText>{suggestion}</SuggestionText>
        </SuggestionContainer>
    );
}
