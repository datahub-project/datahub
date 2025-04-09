import React from 'react';
import colors from '@src/alchemy-components/theme/foundations/colors';
import styled from 'styled-components';
import { ANTD_GRAY } from '../entity/shared/constants';

export const ShowMoreButton = styled.div`
    padding: 4px;
    color: ${colors.gray[1700]};
    text-align: left;
    font-weight: 700;
    font-size: 12px;
    font-family: 'Mulish';
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
