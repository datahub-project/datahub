import React from 'react';
import styled from 'styled-components';

const Container = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
`;

const Column = styled.div`
    font-weight: bold;
    font-size: 12px;
    padding: 12px 0px;
    display: flex;
    flex-direction: column;
    align-items: space-between;
    justify-content: start;
    margin-right: 32px;
`;

const Title = styled.div`
    margin-bottom: 4px;
`;

type SidebarStatsColumn = {
    title: React.ReactNode;
    content: React.ReactNode;
};

type Props = {
    columns: SidebarStatsColumn[];
};

export const SidebarHeaderSectionColumns = ({ columns }: Props) => {
    return (
        <Container>
            {columns.map((column) => (
                <Column>
                    <Title>{column.title}</Title>
                    {column.content}
                </Column>
            ))}
        </Container>
    );
};
