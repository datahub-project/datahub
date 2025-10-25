import { Pill } from '@components';
import React from 'react';
import styled from 'styled-components';

const SuggestionsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-top: 16px;
    margin-left: 4px;
    justify-content: start;
    align-items: start;
    width: 100%;
`;

const SuggestionsLabel = styled.div`
    font-size: 13px;
    font-weight: 500;
    color: #8c8c8c;
    margin-bottom: 4px;
`;

const PillsWrapper = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    justify-content: start;
`;

interface SuggestedQuestionsProps {
    onQuestionSelect: (question: string) => void;
}

const DEFAULT_QUESTIONS = [
    'Show the most highly used tables',
    'Find reports related to sales',
    'Help me build a new dashboard...',
];

export const SuggestedQuestions: React.FC<SuggestedQuestionsProps> = ({ onQuestionSelect }) => {
    return (
        <SuggestionsContainer>
            <SuggestionsLabel>Suggested</SuggestionsLabel>
            <PillsWrapper>
                {DEFAULT_QUESTIONS.map((question) => (
                    <Pill
                        key={question}
                        label={question}
                        variant="outline"
                        color="gray"
                        clickable
                        onPillClick={() => onQuestionSelect(question)}
                    />
                ))}
            </PillsWrapper>
        </SuggestionsContainer>
    );
};
