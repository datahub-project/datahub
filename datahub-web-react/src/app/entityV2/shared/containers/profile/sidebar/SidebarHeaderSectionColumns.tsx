import React from 'react';
import styled from 'styled-components';

import { SidebarStatsColumn } from '@app/entityV2/shared/containers/profile/utils';
import HorizontalScroller from '@app/sharedV2/carousel/HorizontalScroller';

const ColumnsContainer = styled(HorizontalScroller)`
    display: flex;
    flex-direction: row;
    align-items: stretch;
    justify-content: space-between;
    padding: 0 20px;
    margin-top: -8px;
    margin-bottom: -8px;
    overflow: auto;

    & > div {
        &:not(:first-child) {
            border-left: 1px solid ${(props) => props.theme.colors.border};
        }
    }
`;

const Column = styled.div`
    font-weight: bold;
    font-size: 12px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    flex: 1;
    padding-top: 8px;
    padding-bottom: 8px;

    &:not(:first-child) {
        padding-left: 20px;
    }
`;

const Heading = styled.div`
    display: flex;
    gap: 4px;
    align-items: center;
    margin-bottom: 4px;
`;

const Title = styled.div`
    font-size: 12px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
`;

interface Props {
    columns: SidebarStatsColumn[];
}

export const SidebarHeaderSectionColumns = ({ columns }: Props) => {
    if (!columns.length) return null;

    return (
        <ColumnsContainer scrollButtonSize={18} scrollButtonOffset={15}>
            {columns.map((column) => (
                <Column>
                    <Heading>
                        <Title>{column.title}</Title>
                    </Heading>
                    {column.content}
                </Column>
            ))}
        </ColumnsContainer>
    );
};
