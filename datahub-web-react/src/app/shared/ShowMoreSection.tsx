import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';

export const ShowMoreButton = styled.div`
    margin-top: 12px;
    padding: 0px;
    color: ${ANTD_GRAY[7]};
    text-align: left;
    :hover {
        cursor: pointer;
        color: ${ANTD_GRAY[8]};
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
