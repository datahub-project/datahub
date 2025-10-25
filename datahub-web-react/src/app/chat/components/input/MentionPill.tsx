import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

const MentionLink = styled.span`
    color: ${colors.primary[500]};
    font-weight: 500;
    cursor: pointer;

    &:hover {
        text-decoration: underline;
    }
`;

interface Props {
    displayName: string;
    onClick?: () => void;
}

export const MentionPill: React.FC<Props> = ({ displayName, onClick }) => {
    return <MentionLink onClick={onClick}>@{displayName}</MentionLink>;
};
