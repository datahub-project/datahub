import { Button } from '@components';
import React from 'react';
import styled from 'styled-components';

const EmptyStateContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: start;
    text-align: center;
`;

interface DocumentTreeEmptyStateProps {
    onCreateDocument: () => void;
}

export const DocumentTreeEmptyState: React.FC<DocumentTreeEmptyStateProps> = ({ onCreateDocument }) => {
    return (
        <EmptyStateContainer>
            <Button
                icon={{ icon: 'Plus', source: 'phosphor' }}
                variant="text"
                color="gray"
                onClick={onCreateDocument}
                data-testid="empty-state-new-document-button"
            >
                New Document
            </Button>
        </EmptyStateContainer>
    );
};
