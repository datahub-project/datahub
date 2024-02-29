import React from 'react';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '../../../constants';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 10px;
    border-bottom: 1px solid #0000001a;
    margin-left: -20px;
    margin-right: -20px;
`;

const ColumnsContainer = styled.div`
    display: flex;
    flex-direction: row;
    align-items: start;
    justify-content: start;
    margin-left: 5px;

    & > div {
        &:not(:first-child) {
            border-left: 1px dashed;
            border-color: rgba(0, 0, 0, 0.3);
        }
    }
`;

const Column = styled.div`
    font-weight: bold;
    font-size: 12px;
    display: flex;
    flex-direction: column;
    align-items: space-between;
    justify-content: start;
    margin-right: 20px;
    padding-left: 20px;
`;

const Heading = styled.div`
    display: flex;
    gap: 4px;
    color: ${REDESIGN_COLORS.DARK_GREY};
    align-items: center;
    margin-bottom: 4px;
`;

const Title = styled.div`
    font-size: 12px;
    font-weight: 600;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

type SidebarStatsColumn = {
    title: React.ReactNode;
    content: React.ReactNode;
    icon: React.ReactNode;
};

type Props = {
    columns: SidebarStatsColumn[];
};

export const SidebarHeaderSectionColumns = ({ columns }: Props) => {
    return (
        <Container className="top-section">
            {columns.length > 0 && (
                <ColumnsContainer>
                    {columns.map((column) => (
                        <Column>
                            <Heading>
                                <Title>{column.title}</Title>
                            </Heading>
                            {column.content}
                        </Column>
                    ))}
                </ColumnsContainer>
            )}
        </Container>
    );
};
