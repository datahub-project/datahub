import React from 'react';
import styled from 'styled-components';

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
    color: ${(props) => props.theme.colors.text};

    &:hover {
        background-color: ${(props) => props.theme.colors.border};
        color: ${(props) => props.theme.colors.iconBrand};
    }
`;

interface ShowMoreButtonProps {
    onClick: () => void;
}

export const ShowMoreButton: React.FC<ShowMoreButtonProps> = ({ onClick }) => {
    return <ShowMoreContainer onClick={onClick}>Show more...</ShowMoreContainer>;
};
