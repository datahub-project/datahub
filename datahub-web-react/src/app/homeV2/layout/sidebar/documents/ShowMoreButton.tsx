import React from 'react';
import styled from 'styled-components';

import { colors } from '@src/alchemy-components/theme';

const ShowMoreContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: flex-start;
    padding: 4px 8px;
    margin-bottom: 2px;
    margin-left: 2px;
    margin-right: 2px;
    min-height: 38px;
    cursor: pointer;
    border-radius: 6px;
    transition: background-color 0.15s ease;
    font-size: 13px;
    font-weight: 500;
    color: ${colors.gray[600]};

    &:hover {
        background-color: ${colors.gray[100]};
        color: ${colors.violet[500]};
    }
`;

interface ShowMoreButtonProps {
    onClick: () => void;
}

export const ShowMoreButton: React.FC<ShowMoreButtonProps> = ({ onClick }) => {
    return <ShowMoreContainer onClick={onClick}>Show more...</ShowMoreContainer>;
};
