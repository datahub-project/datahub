import { colors } from '@components';
import React from 'react';
import styled from 'styled-components';

const EmptyStateContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: start;
    text-align: center;
    colors: ${colors.gray[1700]};
`;

export const DocumentTreeEmptyState: React.FC = () => {
    return <EmptyStateContainer>No documents yet.</EmptyStateContainer>;
};
