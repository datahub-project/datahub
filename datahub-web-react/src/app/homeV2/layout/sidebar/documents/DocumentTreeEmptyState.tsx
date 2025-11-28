import { Button, colors } from '@components';
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

const PlusButton = styled(Button)`
    color: ${colors.gray[1700]};
    &:hover {
        background-color: ${colors.gray[100]};
    }
`;

interface DocumentTreeEmptyStateProps {
    onCreateDocument: () => void;
}

export const DocumentTreeEmptyState: React.FC<DocumentTreeEmptyStateProps> = ({ onCreateDocument }) => {
    return (
        <EmptyStateContainer>
            <PlusButton
                icon={{ icon: 'Plus', source: 'phosphor' }}
                variant="text"
                onClick={onCreateDocument}
                data-testid="empty-state-new-document-button"
            >
                New Document
            </PlusButton>
        </EmptyStateContainer>
    );
};
