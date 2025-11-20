import { Button } from '@components';
import { Plus } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

import colors from '@src/alchemy-components/theme/foundations/colors';

const EmptyStateContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: start;
    justify-content: start;
    text-align: center;
`;

const EmptyStateText = styled.div`
    color: ${colors.gray[1700]};
    font-family: Mulish;
    font-size: 13px;
    font-weight: 400;
    margin-bottom: 16px;
    line-height: 1.4;
`;

const NewDocumentButton = styled.button`
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 8px 16px;
    background: ${colors.violet[500]};
    color: white;
    border: none;
    border-radius: 6px;
    font-family: Mulish;
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s ease;

    &:hover {
        background: ${colors.violet[600]};
        transform: translateY(-1px);
    }

    &:active {
        transform: translateY(0);
    }

    svg {
        width: 16px;
        height: 16px;
    }
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
