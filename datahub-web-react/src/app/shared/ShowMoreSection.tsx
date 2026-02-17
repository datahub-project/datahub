import React from 'react';
import styled from 'styled-components';


export const ShowMoreButton = styled.div`
    padding: 4px;
    color: ${(props) => props.theme.colors.textSecondary};
    text-align: left;
    font-weight: 700;
    font-size: 12px;
    font-family: 'Mulish';
    :hover {
        cursor: pointer;
        color: ${(props) => props.theme.colors.textSecondary};
        text-decoration: underline;
    }
`;

type Props = {
    totalCount: number;
    visibleCount: number;
    setVisibleCount: (visibleCount: number) => void;
    pageSize?: number;
};

export const ShowMoreSection = ({ totalCount, visibleCount, setVisibleCount, pageSize = 4 }: Props) => {
    const showMoreCount = visibleCount + pageSize > totalCount ? totalCount - visibleCount : pageSize;
    return (
        <ShowMoreButton onClick={() => setVisibleCount(visibleCount + pageSize)}>
            {showMoreCount ? `show ${showMoreCount} more` : 'show more'}
        </ShowMoreButton>
    );
};
